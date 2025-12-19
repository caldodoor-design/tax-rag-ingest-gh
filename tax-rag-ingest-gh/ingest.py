import os
import yaml
import hashlib
import inspect
from typing import Dict, List, Tuple

from tqdm import tqdm

from text_utils import chunk_text, clean_text
from egov import collect_laws_by_keywords
from nta import crawl_nta
from kfs import collect_kfs_saiketsu
from embed import embed_texts
from upsert import upsert_documents_and_chunks


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _call_collect_laws_by_keywords(cfg_egov: Dict) -> List[Dict]:
    """
    egov.collect_laws_by_keywords の引数が版で揺れても落ちないように
    signatureを見て渡せるものだけ渡す。
    """
    sig = inspect.signature(collect_laws_by_keywords)
    kwargs = {}

    if "keywords" in sig.parameters:
        kwargs["keywords"] = cfg_egov.get("keywords", [])

    if "max_laws" in sig.parameters:
        kwargs["max_laws"] = int(cfg_egov.get("max_laws", 500))

    if "category" in sig.parameters and cfg_egov.get("category") is not None:
        kwargs["category"] = int(cfg_egov.get("category"))

    for key in ["exact_allow", "prefix_allow", "include_suffixes", "exclude_phrases"]:
        if key in sig.parameters and cfg_egov.get(key) is not None:
            kwargs[key] = cfg_egov.get(key)

    return collect_laws_by_keywords(**kwargs)


def _crawl_html_block(cfg_block: Dict, kind: str) -> List[Dict]:
    """
    www.nta.go.jp の静的HTMLを crawl_nta でクロールする共通ブロック
    (基本通達 / 措置法通達 / 質疑応答 / TaxAnswer / 個別通達など)
    """
    return crawl_nta(
        seeds=cfg_block.get("seeds", []),
        max_pages=int(cfg_block.get("max_pages", 1000)),
        delay_seconds=float(cfg_block.get("delay_seconds", 0.6)),
        allowed_prefixes=cfg_block.get("allowed_prefixes"),
        exclude_url_regex=cfg_block.get("exclude_url_regex"),
        extra_defaults={"nta_kind": kind},
        skip_save_title_regex=cfg_block.get("skip_save_title_regex"),
        skip_save_url_regex=cfg_block.get("skip_save_url_regex"),
    )


def main():
    cfg = load_config("sources.yaml")
    docs: List[Dict] = []

    # 1) e-Gov
    if cfg.get("egov", {}).get("enabled", False):
        docs.extend(_call_collect_laws_by_keywords(cfg["egov"]))

    # 2) NTA 基本通達
    if cfg.get("nta", {}).get("enabled", False):
        docs.extend(_crawl_html_block(cfg["nta"], kind="kihon"))

    # 3) NTA 措置法通達
    if cfg.get("nta_sochiho", {}).get("enabled", False):
        docs.extend(_crawl_html_block(cfg["nta_sochiho"], kind="sochiho"))

    # 4) NTA 質疑応答事例
    if cfg.get("nta_shitsugi", {}).get("enabled", False):
        docs.extend(_crawl_html_block(cfg["nta_shitsugi"], kind="shitsugi"))

    # 5) Tax Answer（タックスアンサー）
    if cfg.get("taxanswer", {}).get("enabled", False):
        docs.extend(_crawl_html_block(cfg["taxanswer"], kind="taxanswer"))

    # 6) NTA 個別通達
    if cfg.get("nta_kobetsu", {}).get("enabled", False):
        docs.extend(_crawl_html_block(cfg["nta_kobetsu"], kind="kobetsu"))

    # 7) KFS 公表裁決事例（裁決事例）
    if cfg.get("kfs_saiketsu", {}).get("enabled", False):
        kk = cfg["kfs_saiketsu"]
        docs.extend(
            collect_kfs_saiketsu(
                start_url=kk.get("start_url", "https://www.kfs.go.jp/service/JP/index.html"),
                delay_seconds=float(kk.get("delay_seconds", 1.2)),
                max_cases=int(kk.get("max_cases", 0)),
                include_youshi=bool(kk.get("include_youshi", False)),
            )
        )

    # ---- normalize docs ----
    normalized_docs: List[Dict] = []
    for d in docs:
        source = d.get("source", "unknown")
        url = (d.get("url") or "").strip()
        title = (d.get("title") or url).strip()

        if not url:
            continue

        content = clean_text(d.get("content", ""))
        if not content or len(content) < 80:
            continue

        doc_id = sha1(f"{source}|{url}")
        content_hash = sha1(content)

        normalized_docs.append(
            {
                "id": doc_id,
                "source": source,
                "title": title,
                "url": url,
                "content_hash": content_hash,
                "content": content,  # chunk化用に一時保持
            }
        )

    # ---- chunking ----
    ch_cfg = cfg.get("chunking", {}) or {}
    max_chars = int(ch_cfg.get("max_chars", 1200))
    overlap = int(ch_cfg.get("overlap_chars", 200))

    all_chunk_texts: List[str] = []
    all_chunk_refs: List[Tuple[str, int, str, str]] = []  # (doc_id, idx, text, hash)

    for d in normalized_docs:
        chunks = chunk_text(d["content"], max_chars=max_chars, overlap_chars=overlap)
        for i, c in enumerate(chunks):
            h = sha1(c)
            all_chunk_texts.append(c)
            all_chunk_refs.append((d["id"], i, c, h))

    # ---- embedding ----
    emb_cfg = cfg.get("embedding", {}) or {}
    model_name = emb_cfg.get("model", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    normalize = bool(emb_cfg.get("normalize", True))
    batch = int(emb_cfg.get("batch_size", 64))

    embeddings: List[List[float]] = []
    for i in tqdm(range(0, len(all_chunk_texts), batch), desc="Embedding"):
        embeddings.extend(
            embed_texts(
                all_chunk_texts[i : i + batch],
                model_name=model_name,
                normalize=normalize,
            )
        )

    chunks_by_doc: Dict[str, List[Dict]] = {}
    for (doc_id, idx, c, h), emb in zip(all_chunk_refs, embeddings):
        chunks_by_doc.setdefault(doc_id, []).append(
            {
                "chunk_index": idx,
                "content": c,
                "content_hash": h,
                "embedding": emb,
            }
        )

    # ---- upsert ----
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable")

    docs_meta = [
        {
            "id": d["id"],
            "source": d["source"],
            "title": d["title"],
            "url": d["url"],
            "content_hash": d["content_hash"],
        }
        for d in normalized_docs
    ]

    print(f"Docs: {len(docs_meta)} / Chunks: {len(all_chunk_refs)}")
    upsert_documents_and_chunks(db_url=db_url, docs=docs_meta, chunks_by_doc=chunks_by_doc)
    print("Done.")


if __name__ == "__main__":
    main()
