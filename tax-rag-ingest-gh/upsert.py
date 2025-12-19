from typing import Dict, List
import hashlib

import psycopg2
from psycopg2.extras import execute_values


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def vec_literal(v: List[float]) -> str:
    # pgvector literal: [0.1,0.2,...]
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def upsert_documents_and_chunks(db_url: str, docs: List[Dict], chunks_by_doc: Dict[str, List[Dict]]):
    if not docs:
        print("No docs to upsert.")
        return

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # ---- documents: UPSERT ----
            doc_rows = []
            doc_ids = []
            for d in docs:
                doc_ids.append(d["id"])
                doc_rows.append((
                    d["id"],
                    d.get("source", "unknown"),
                    d.get("title", ""),
                    d.get("url", ""),
                    d.get("content_hash", ""),
                ))

            execute_values(
                cur,
                """
                insert into public.documents (id, source, title, url, content_hash)
                values %s
                on conflict (id) do update set
                  source = excluded.source,
                  title = excluded.title,
                  url = excluded.url,
                  content_hash = excluded.content_hash
                """,
                doc_rows,
                page_size=500,
            )

            # ---- chunks: このdoc_id群は作り直す（index変動/削除漏れ防止） ----
            # ※これがあるので、同じdocを再実行しても落ちない
            cur.execute("delete from public.chunks where doc_id = any(%s)", (doc_ids,))

            # ---- chunks: INSERT (dedupeしてから) ----
            chunk_map: Dict[tuple, tuple] = {}
            for doc_id, chs in chunks_by_doc.items():
                for ch in chs:
                    key = (doc_id, int(ch["chunk_index"]))
                    chunk_map[key] = (
                        doc_id,
                        int(ch["chunk_index"]),
                        ch.get("content", ""),
                        ch.get("content_hash", ""),
                        vec_literal(ch["embedding"]),
                    )

            chunk_rows = list(chunk_map.values())

            if chunk_rows:
                execute_values(
                    cur,
                    """
                    insert into public.chunks (doc_id, chunk_index, content, content_hash, embedding)
                    values %s
                    on conflict (doc_id, chunk_index) do update set
                      content = excluded.content,
                      content_hash = excluded.content_hash,
                      embedding = excluded.embedding
                    """,
                    chunk_rows,
                    template="(%s, %s, %s, %s, %s::vector)",
                    page_size=500,
                )
def fetch_existing_docs(conn, source: str):
    """
    既存の documents を {url: content_hash} で返す
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select url, content_hash
            from public.documents
            where source = %s
            """,
            (source,),
        )
        rows = cur.fetchall()
    return {url: h for (url, h) in rows}

        
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
