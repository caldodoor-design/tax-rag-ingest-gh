import hashlib
from typing import Dict, List
import psycopg2
import psycopg2.extras

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def vec_literal(v: List[float]) -> str:
    # pgvector literal: [0.1,0.2,...]
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"

def upsert_documents_and_chunks(db_url: str, docs: List[Dict], chunks_by_doc: Dict[str, List[Dict]]):
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Upsert documents
            for doc in docs:
                cur.execute(
                    """
                    insert into public.documents (id, source, title, url, retrieved_at, content_hash, is_active)
                    values (%s, %s, %s, %s, now(), %s, true)
                    on conflict (id) do update set
                      source = excluded.source,
                      title = excluded.title,
                      url = excluded.url,
                      retrieved_at = excluded.retrieved_at,
                      content_hash = excluded.content_hash,
                      is_active = true
                    """,
                    (doc["id"], doc["source"], doc.get("title"), doc["url"], doc["content_hash"]),
                )

            # Upsert chunks
            for doc_id, chunks in chunks_by_doc.items():
                for ch in chunks:
                    cur.execute(
                        """
                        insert into public.chunks (doc_id, chunk_index, content, content_hash, embedding, retrieved_at)
                        values (%s, %s, %s, %s, %s::vector, now())
                        on conflict (doc_id, chunk_index) do update set
                          content = excluded.content,
                          content_hash = excluded.content_hash,
                          embedding = excluded.embedding,
                          retrieved_at = excluded.retrieved_at
                        """,
                        (doc_id, ch["chunk_index"], ch["content"], ch["content_hash"], vec_literal(ch["embedding"])),
                    )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
