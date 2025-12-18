from typing import Dict, List, Any, Tuple
import hashlib

import psycopg2
from psycopg2.extras import execute_values


def sha1(x: Any) -> str:
    """
    ingest.py が import するやつ。文字列(UTF-8)でSHA1 hexを返す。
    """
    if x is None:
        x = ""
    if isinstance(x, (bytes, bytearray)):
        b = bytes(x)
    else:
        b = str(x).encode("utf-8")
    return hashlib.sha1(b).hexdigest()


def _doc_id(doc: Dict[str, Any]) -> str:
    # 安定ID：idが無ければ source:url
    return doc.get("id") or f"{doc['source']}:{doc['url']}"


def _vec_to_pgvector(v) -> str:
    # list/tuple/numpy を想定して pgvector の文字列へ
    if hasattr(v, "tolist"):
        v = v.tolist()
    return "[" + ",".join(str(float(x)) for x in v) + "]"


def upsert_documents_and_chunks(
    db_url: str,
    docs: List[Dict[str, Any]],
    chunks_by_doc: Dict[str, List[Dict[str, Any]]],
):
    if not docs:
        print("[upsert] no docs")
        return

    # doc_id と content_hash を確定
    normalized_docs: List[Dict[str, Any]] = []
    for d in docs:
        dd = dict(d)
        dd["id"] = _doc_id(dd)
        # 念のため content_hash が無い場合は本文から作る
        if not dd.get("content_hash"):
            dd["content_hash"] = sha1(dd.get("content", ""))
        normalized_docs.append(dd)

    doc_ids = [d["id"] for d in normalized_docs]

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                # 既存hashを引いて「変更があるdocだけ」処理する
                cur.execute(
                    "select id, content_hash from public.documents where id = any(%s)",
                    (doc_ids,),
                )
                existing = {row[0]: row[1] for row in cur.fetchall()}

                changed_ids: List[str] = []
                docs_rows: List[Tuple] = []

                for d in normalized_docs:
                    did = d["id"]
                    new_hash = d["content_hash"]
                    old_hash = existing.get(did)
                    if old_hash == new_hash:
                        continue  # ✅ 同じなら何もしない（上書きしない）
                    changed_ids.append(did)
                    docs_rows.append(
                        (
                            did,
                            d["source"],
                            d.get("title"),
                            d["url"],
                            new_hash,
                            True,
                        )
                    )

                if not changed_ids:
                    print("[upsert] no changes (skip)")
                    return

                # documents: 内容が変わったものだけ upsert
                execute_values(
                    cur,
                    """
                    insert into public.documents (id, source, title, url, content_hash, is_active)
                    values %s
                    on conflict (id) do update
                      set source=excluded.source,
                          title=excluded.title,
                          url=excluded.url,
                          content_hash=excluded.content_hash,
                          retrieved_at=now(),
                          is_active=true
                    where public.documents.content_hash is distinct from excluded.content_hash
                    """,
                    docs_rows,
                )

                # chunks: 変更があったdocだけ差し替え（未変更は触らない）
                cur.execute(
                    "delete from public.chunks where doc_id = any(%s)",
                    (changed_ids,),
                )

                chunk_rows: List[Tuple] = []
                for did in changed_ids:
                    chunks = chunks_by_doc.get(did, [])
                    for c in chunks:
                        content = c.get("content", "")
                        chash = c.get("content_hash") or sha1(content)
                        emb = c["embedding"]
                        chunk_rows.append(
                            (
                                did,
                                int(c["chunk_index"]),
                                content,
                                chash,
                                _vec_to_pgvector(emb),
                            )
                        )

                # 大量insertは分割して進捗を出す（安心用）
                PAGE = 500
                total = len(chunk_rows)
                for i in range(0, total, PAGE):
                    batch = chunk_rows[i : i + PAGE]
                    execute_values(
                        cur,
                        """
                        insert into public.chunks (doc_id, chunk_index, content, content_hash, embedding)
                        values %s
                        """,
                        batch,
                        template="(%s,%s,%s,%s,%s::vector)",
                        page_size=len(batch),
                    )
                    print(f"[upsert] chunks inserted: {i+len(batch)}/{total}")

        print(f"[upsert] updated docs={len(changed_ids)} chunks={len(chunk_rows)}")
    finally:
        conn.close()
