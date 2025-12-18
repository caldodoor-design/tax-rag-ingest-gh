import os
import yaml
from tqdm import tqdm
from typing import Dict, List

from text_utils import chunk_text, clean_text
from egov import collect_laws_by_keywords
from nta import crawl_nta
from embed import embed_texts
from upsert import sha1, upsert_documents_and_chunks


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main():
    cfg = load_config("sources.yaml")

    docs: List[Dict] = []

    # 1) e-Gov laws
    if cfg.get("egov", {}).get("enabled", False):
        eg = cfg["egov"]
        keywords = eg.get("keywords", [])
        max_laws = int(eg.get("max_laws", 500))
        category = int(eg.get("category", 1))
        docs.extend(
            collect_laws_by_keywords(
                keywords=keywords,
                max_laws=max_laws,
                category=category,
                exact_allow=eg.get("exact_allow"),
                prefix_allow=eg.get("prefix_allow"),
                include_suffixes=eg.get("include_suffixes"),
                exclude_phrases=eg.get("exclude_phrases"),
            )
        )

    # 2) NTA crawl（基本通達）
    if cfg.get("nta", {}).get("enabled", False):
        nt = cfg["nta"]
        seeds = nt.get("seeds", [])
        max_pages = int(nt.get("max_pages", 100))
        delay = float(nt.get("delay_seconds", 0.6))
        docs.extend(
            crawl_nta(
                seeds=seeds,
                max_pages=max_pages,
                delay_seconds=delay,
                allowed_prefixes=nt.get("allowed_prefixes"),
                exclude_url_regex=nt.get("exclude_url_regex"),
                extra_defaults={"nta_kind":"kihon"}
                skip_save_title_regex=nt.get("skip_save_title_regex"),
                skip_save_url_regex=nt.get("skip_save_url_regex"),

            )
        )

    # 3) NTA sochiho（措置法通達）
    if cfg.get("nta_sochiho", {}).get("enabled", False):
        nt = cfg["nta_sochiho"]
        docs.extend(
            crawl_nta(
                seeds=nt.get("seeds", []),
                max_pages=int(nt.get("max_pages", 2500)),
                delay_seconds=float(nt.get("delay_seconds", 0.6)),
                allowed_prefixes=nt.get("allowed_prefixes"),
                exclude_url_regex=nt.get("exclude_url_regex"),
                extra_defaults={"nta_kind": "sochiho"},
                skip_save_title_regex=nt.get("skip_save_title_regex"),
                skip_save_url_regex=nt.get("skip_save_url_regex"),

            )
        )
   # 4) NTA shitsugi（質疑応答事例）
   if cfg.get("nta_shitsugi", {}).get("enabled", False):
        nt = cfg["nta_shitsugi"]
        docs.extend(
            crawl_nta(
                seeds=nt.get("seeds", []),
                max_pages=int(nt.get("max_pages", 2000)),
                delay_seconds=float(nt.get("delay_seconds", 0.6)),
                allowed_prefixes=nt.get("allowed_prefixes"),
                exclude_url_regex=nt.get("exclude_url_regex"),
                extra_defaults={"nta_kind": "shitsugi"},
                skip_save_title_regex=nt.get("skip_save_title_regex"),
                skip_save_url_regex=nt.get("skip_save_url_regex"),
            )
        )

    
    # normalize and id/hash
    normalized_docs: List[Dict] = []
    for d in docs:
        content = clean_text(d.get("content", ""))
        if not content or len(content) < 80:
            continue
        doc_id = sha1(f"{d['source']}|{d['url']}")
        content_hash = sha1(content)
        normalized_docs.append(
            {
                "id": doc_id,
                "source": d["source"],
                "title": d.get("title") or d["url"],
                "url": d["url"],
                "content": content,
                "content_hash": content_hash,
                "extra": d.get("extra", {}),
            }
        )

    # chunk
    ch_cfg = cfg.get("chunking", {})
    max_chars = int(ch_cfg.get("max_chars", 1200))
    overlap = int(ch_cfg.get("overlap_chars", 200))

    chunks_by_doc: Dict[str, List[Dict]] = {}
    all_chunk_texts: List[str] = []
    all_chunk_refs: List[tuple] = []  # (doc_id, chunk_index, content, content_hash)

    for d in normalized_docs:
        chunks = chunk_text(d["content"], max_chars=max_chars, overlap_chars=overlap)
        for i, c in enumerate(chunks):
            h = sha1(c)
            all_chunk_texts.append(c)
            all_chunk_refs.append((d["id"], i, c, h))

    # embedding
    emb_cfg = cfg.get("embedding", {})
    model_name = emb_cfg.get(
        "model", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    normalize = bool(emb_cfg.get("normalize", True))

    embeddings: List[List[float]] = []
    batch = 64
    for i in tqdm(range(0, len(all_chunk_texts), batch), desc="Embedding"):
        embeddings.extend(
            embed_texts(
                all_chunk_texts[i : i + batch],
                model_name=model_name,
                normalize=normalize,
            )
        )

    # assign embeddings to chunks_by_doc
    for (doc_id, idx, c, h), emb in zip(all_chunk_refs, embeddings):
        chunks_by_doc.setdefault(doc_id, []).append(
            {
                "chunk_index": idx,
                "content": c,
                "content_hash": h,
                "embedding": emb,
            }
        )

    # upsert to DB
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable")

    print(f"Docs: {len(normalized_docs)} / Chunks: {len(all_chunk_refs)}")
    upsert_documents_and_chunks(db_url=db_url, docs=normalized_docs, chunks_by_doc=chunks_by_doc)
    print("Done.")


if __name__ == "__main__":
    main()
