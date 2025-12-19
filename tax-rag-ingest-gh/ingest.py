import os
import yaml
import inspect
from typing import Dict, List, Tuple, Optional

import psycopg2
from tqdm import tqdm

from text_utils import chunk_text, clean_text
from egov import collect_laws_by_keywords
from nta import crawl_nta
from embed import embed_texts
from upsert import sha1, upsert_documents_and_chunks

# KFS（裁決事例）対応：kfs.py がある環境だけ有効になるように
_CRAWL_KFS = None
try:
    from kfs import crawl_kfs as _CRAWL_KFS  # type: ignore
except Exception:
    try:
        from kfs import collect_kfs as _CRAWL_KFS  # type: ignore
    except Exception:
        try:
            from kfs import crawl_kfs_decisions as _CRAWL_KFS  # type: ignore
        except Exception:
            _CRAWL_KFS = None


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def call_collect_laws_by_keywords(eg_cfg: Dict) -> List[Dict]:
    """egov.collect_laws_by_keywords の引数揺れに耐える呼び出し"""
    sig = inspect.signature(collect_laws_by_keywords)
    kwargs = {}

    if "keywords" in sig.parameters:
        kwargs["keywords"] = eg_cfg.get("keywords", [])

    if "max_laws" in sig.parameters:
        kwargs["max_laws"] = int(eg_cfg.get("max_laws", 500))

    if "category" in sig.parameters and eg_cfg.get("category") is not None:
        kwargs["category"] = int(eg_cfg.get("category", 1))

    # フィルタ系（存在するものだけ渡す）
    for k in ["exact_allow", "prefix_allow", "include_suffixes", "exclude_phrases"]:
        if k in sig.parameters and eg_cfg.get(k) is not None:
            kwargs[k] = eg_cfg.get(k)

    return collect_laws_by_keywords(**kwargs)


def call_crawl_nta(block_cfg: Dict, kind: str) -> List[Dict]:
    """nta.crawl_nta の引数揺れに耐える呼び出し（目次は保存しない等も対応）"""
    sig = inspect.signature(crawl_nta)
    kwargs = {}

    # 必須級
    if "seeds" in sig.parameters:
        kwargs["seeds"] = block_cfg.get("seeds", [])
    if "max_pages" in sig.parameters:
        kwargs["max_pages"] = int(block_cfg.get("max_pages", 1000))
    if "delay_seconds" in sig.parameters:
        kwargs["delay_seconds"] = float(block_cfg.get("delay_seconds", 0.6))

    # 任意
    if "allowed_prefixes" in sig.parameters:
        kwargs["allowed_prefixes"] = block_cfg.get("allowed_prefixes")
    if "exclude_url_regex" in sig.parameters:
        kwargs["exclude_url_regex"] = block_cfg.get("exclude_url_regex")

    # 追加メタ（対応してる版だけ）
    if "extra_defaults" in sig.parameters:
        kwargs["extra_defaults"] = {"nta_kind": kind}

    # 「目次/一覧は保存しない」（対応してる版だけ）
    if "skip_save_title_regex" in sig.parameters:
        kwargs["skip_save_title_regex"] = block_cfg.get("skip_save_title_regex")
    if "skip_save_url_regex" in sig.parameters:
        kwargs["skip_save_url_regex"] = block_cfg.get("skip_save_url_regex")

    return crawl_nta(**kwargs)


def call_crawl_kfs(block_cfg: Dict) -> List[Dict]:
    """kfs 側の関数名/引数揺れに耐える呼び出し"""
    if _CRAWL_KFS is None:
        raise RuntimeError("kfs.py が見つからない or crawl関数が import できません")

    sig = inspect.signature(_CRAWL_KFS)
    kwargs = {}

    # よくある引数だけ、存在するものを渡す
    for k in ["seeds", "start_urls"]:
        if k in sig.parameters:
            kwargs[k] = block_cfg.get("seeds", block_cfg.get("start_urls", []))

    for k in ["max_pages", "limit"]:
        if k in sig.parameters:
            kwargs[k] = int(block_cfg.get("max_pages", block_cfg.get("limit", 5000)))

    for k in ["delay_seconds", "delay"]:
        if k in sig.parameters:
            kwargs[k] = float(block_cfg.get("delay_seconds", block_cfg.get("delay", 0.6)))

    for k in ["allowed_prefixes", "exclude_url_regex", "skip_save_title_regex", "skip_save_url_regex"]:
        if k in sig.parameters:
            kwargs[k] = block_cfg.get(k)

    return _CRAWL_KFS(**kwargs)


def fetch_existing_hashes(conn, sources: List[str]) -> Dict[Tuple[str, str], str]:
    """DBに既にある (source,url)->content_hash を取る"""
    if not sources:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select source, url, content_hash
            from public.documents
            where source = any(%s)
            """,
            (sources,),
        )
        rows = cur.fetchall()
    return {(s, u): h for (s, u, h) in rows}


def delete_chunks_for_docs(conn, doc_ids: List[str]) -> None:
    """変更のあったdocの古いchunksを先に全削除（chunk数が減るときのゴミ防止）"""
    if not doc_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "delete from public.chunks where doc_id = any(%s)",
            (doc_ids,),
        )


def main():
    # config
    cfg_path = os.environ.get("SOURCES_YAML", "sources.yaml")
    cfg = load_config(cfg_path)

    # DB URL（差分判定にも使うので先に必須化）
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable")

    docs: List[Dict] = []

    # 1) e-Gov
    if cfg.get("egov", {}).get("enabled", False):
        docs.extend(call_collect_laws_by_keywords(cfg["egov"]))

    # 2) NTA: 基本通達
    if cfg.get("nta", {}).get("enabled", False):
        docs.extend(call_crawl_nta(cfg["nta"], kind="kihon"))

    # 3) NTA: 措置法通達（任意）
    if cfg.get("nta_sochiho", {}).get("enabled", False):
        docs.extend(call_crawl_nta(cfg["nta_sochiho"], kind="sochiho"))

    # 4) NTA: 質疑応答事例（任意）
    if cfg.get("nta_shitsugi", {}).get("enabled", False):
        docs.extend(call_crawl_nta(cfg["nta_shitsugi"], kind="shitsugi"))

    # 5) NTA: タックスアンサー（任意）
    if cfg.get("taxanswer", {}).get("enabled", False):
        docs.extend(call_crawl_nta(cfg["taxanswer"], kind="taxanswer"))

    # 6) NTA: 個別通達（任意）
    if cfg.get("nta_kobetsu", {}).get("enabled", False):
        docs.extend(call_crawl_nta(cfg["nta_kobetsu"], kind="kobetsu"))

    # 7) KFS: 裁決事例（任意）
    if cfg.get("kfs", {}).get("enabled", False):
        docs.extend(call_crawl_kfs(cfg["kfs"]))

    # ---- normalize ----
    normalized: List[Dict] = []
    for d in docs:
        source = d.get("source", "unknown")
        url = d.get("url", "")
        title = d.get("title") or url

        content = clean_text(d.get("content", ""))
        if not content or len(content) < 80:
            continue

        doc_id = sha1(f"{source}|{url}")
        content_hash = sha1(content)

        normalized.append(
            {
                "id": doc_id,
                "source": source,
                "title": title,
                "url": url,
                "content_hash": content_hash,
                "content": content,  # chunk用に一時保持
            }
        )

    # ---- diff mode（差分だけ）----
    diff_cfg = cfg.get("diff", {}) or {}
    diff_enabled = bool(diff_cfg.get("enabled", True))

    total_fetched = len(normalized)
    changed_docs = normalized

    if diff_enabled and total_fetched > 0:
        sources = sorted(list({d["source"] for d in normalized}))
        conn = psycopg2.connect(db_url)
        try:
            existing = fetch_existing_hashes(conn, sources)
            changed_docs = [
                d for d in normalized
                if existing.get((d["source"], d["url"])) != d["content_hash"]
            ]
        finally:
            conn.close()

    print(f"Docs total: {total_fetched} / Changed: {len(changed_docs)}")

    if len(changed_docs) == 0:
        print("No changes. Done.")
        return

    # ---- chunking ----
    ch_cfg = cfg.get("chunking", {}) or {}
    max_chars = int(ch_cfg.get("max_chars", 1200))
    overlap = int(ch_cfg.get("overlap_chars", 200))

    all_chunk_texts: List[str] = []
    all_chunk_refs: List[Tuple[str, int, str, str]] = []  # (doc_id, idx, content, hash)

    for d in changed_docs:
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

    # ---- delete old chunks for changed docs (important) ----
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        delete_chunks_for_docs(conn, [d["id"] for d in changed_docs])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # ---- upsert ----
    docs_meta = [
        {
            "id": d["id"],
            "source": d["source"],
            "title": d["title"],
            "url": d["url"],
            "content_hash": d["content_hash"],
        }
        for d in changed_docs
    ]

    print(f"Upserting Docs: {len(docs_meta)} / Chunks: {len(all_chunk_refs)}")
    upsert_documents_and_chunks(db_url=db_url, docs=docs_meta, chunks_by_doc=chunks_by_doc)
    print("Done.")


if __name__ == "__main__":
    main()
