import time
from typing import Dict, List, Optional, Set

import requests

BASE_V2 = "https://laws.e-gov.go.jp/api/2"

def _extract_text(node) -> str:
    """
    e-Gov v2 の law_full_text (JSONツリー) から文字だけを再帰的に抽出する
    """
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        parts = []
        for x in node:
            t = _extract_text(x)
            if t:
                parts.append(t)
        return "\n".join(parts)
    if isinstance(node, dict):
        return _extract_text(node.get("children"))
    return str(node)

def _get_json(session: requests.Session, url: str, params: Dict, timeout: int = 60) -> Optional[Dict]:
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[eGov] request failed: {url} params={params} err={e}")
        return None

def search_laws_by_title(session: requests.Session, law_title: str, limit: int = 5) -> List[Dict]:
    """
    法令一覧取得（v2）
    GET /laws?law_title=...&limit=...&response_format=json
    """
    data = _get_json(
        session,
        f"{BASE_V2}/laws",
        params={
            "law_title": law_title,
            "limit": limit,
            "response_format": "json",
        },
    )
    if not data:
        return []
    return data.get("laws", []) or []

def fetch_law_full_text(session: requests.Session, law_id: str) -> Optional[Dict]:
    """
    法令本文取得（v2）
    GET /law_data/{law_id}?response_format=json&law_full_text_format=json
    """
    return _get_json(
        session,
        f"{BASE_V2}/law_data/{law_id}",
        params={
            "response_format": "json",
            "law_full_text_format": "json",
        },
    )

def collect_laws_by_keywords(
    keywords: List[str],
    max_laws: int = 30,
    per_keyword_limit: int = 5,
    delay_seconds: float = 0.3,
    category: Optional[int] = None,
    **_ignored,
) -> List[Dict[str, str]]:

    """
    keywords(法令名) → /laws で law_id を引いて → /law_data/{law_id} で本文を取得して docs にして返す
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "tax-rag-mvp/0.1 (+https://example.invalid)"})

    docs: List[Dict[str, str]] = []
    seen_law_ids: Set[str] = set()

    for kw in keywords:
        print(f"[eGov] searching: {kw}")
        laws = search_laws_by_title(session, law_title=kw, limit=per_keyword_limit)

        for item in laws:
            law_info = item.get("law_info", {}) or {}
            revision_info = item.get("revision_info", {}) or {}

            law_id = law_info.get("law_id")
            if not law_id or law_id in seen_law_ids:
                continue
            seen_law_ids.add(law_id)

            title = revision_info.get("law_title") or kw
            law_num = law_info.get("law_num") or ""
            promulgation_date = law_info.get("promulgation_date") or ""

            data = fetch_law_full_text(session, law_id=law_id)
            if not data:
                continue

            full_text_node = data.get("law_full_text")
            text = _extract_text(full_text_node).strip()
            if not text:
                continue

            # 表示用URL（閲覧サイト）
            url = f"https://laws.e-gov.go.jp/law/{law_id}"

            docs.append({
                "source": "egov",
                "title": f"{title}（{law_num}）" if law_num else title,
                "url": url,
                "content": text,
                "extra": {
                    "law_id": law_id,
                    "law_num": law_num,
                    "promulgation_date": promulgation_date,
                },
            })

            print(f"[eGov] fetched: {title} chars={len(text)}")
            time.sleep(delay_seconds)

            if len(docs) >= max_laws:
                return docs

    return docs
