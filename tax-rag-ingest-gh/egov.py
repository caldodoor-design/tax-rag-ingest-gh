import time
from typing import Dict, List, Optional, Set

import requests

BASE_V2 = "https://laws.e-gov.go.jp/api/2"

EGOV_EXCLUDE_PHRASES: List[str] = [
    "の一部を改正する法律", "等の一部を改正する法律",
    "整備法", "改正法", "廃止",
    "特別会計", "交付金", "特例公債", "地方交付税",
]

def _extract_text(node) -> str:
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

def search_laws_by_title(session: requests.Session, law_title: str, limit: int = 30) -> List[Dict]:
    data = _get_json(
        session,
        f"{BASE_V2}/laws",
        params={"law_title": law_title, "limit": limit, "response_format": "json"},
    )
    if not data:
        return []
    return data.get("laws", []) or []

def fetch_law_full_text(session: requests.Session, law_id: str) -> Optional[Dict]:
    return _get_json(
        session,
        f"{BASE_V2}/law_data/{law_id}",
        params={"response_format": "json", "law_full_text_format": "json"},
    )

def _is_excluded(title: str) -> bool:
    return any(p in title for p in EGOV_EXCLUDE_PHRASES)

def _wanted_titles_for_keyword(keyword: str) -> List[str]:
    titles = [keyword]

    # 「◯◯法」「◯◯法律」は施行令・施行規則も取得
    if keyword.endswith("法") or keyword.endswith("法律"):
        titles.append(keyword + "施行令")
        titles.append(keyword + "施行規則")

    # 民法は「施行法」も実務的に欲しいことが多い
    if keyword == "民法":
        titles.append("民法施行法")

    return list(dict.fromkeys(titles))  # unique, keep order

def _pick_exact_title(items: List[Dict], exact_title: str) -> Optional[Dict]:
    # /laws のレスポンスから「完全一致タイトル」の law_id を1つ拾う
    for item in items:
        law_info = item.get("law_info", {}) or {}
        rev_info = item.get("revision_info", {}) or {}
        title = (rev_info.get("law_title") or "").strip()
        law_id = law_info.get("law_id")
        law_num = (law_info.get("law_num") or "").strip()
        if not law_id or not title:
            continue
        if title != exact_title:
            continue
        if _is_excluded(title):
            continue
        return {"law_id": law_id, "title": title, "law_num": law_num}
    return None

def collect_laws_by_keywords(
    keywords: List[str],
    max_laws: int = 200,
    per_title_limit: int = 30,
    delay_seconds: float = 0.25,
    category: Optional[int] = None,
    **_ignored,
) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "tax-rag-mvp/0.2 (+https://example.invalid)"})

    docs: List[Dict[str, str]] = []
    seen_law_ids: Set[str] = set()

    for kw in keywords:
        wanted_titles = _wanted_titles_for_keyword(kw)

        for title_query in wanted_titles:
            print(f"[eGov] searching: {title_query}")
            items = search_laws_by_title(session, law_title=title_query, limit=per_title_limit)

            picked = _pick_exact_title(items, exact_title=title_query)
            if not picked:
                print(f"[eGov] NOT FOUND (or excluded): {title_query}")
                time.sleep(delay_seconds)
                continue

            law_id = picked["law_id"]
            if law_id in seen_law_ids:
                continue
            seen_law_ids.add(law_id)

            data = fetch_law_full_text(session, law_id=law_id)
            if not data:
                print(f"[eGov] failed to fetch law_data: {title_query} id={law_id}")
                time.sleep(delay_seconds)
                continue

            text = _extract_text(data.get("law_full_text")).strip()
            if not text:
                print(f"[eGov] empty text: {title_query} id={law_id}")
                time.sleep(delay_seconds)
                continue

            url = f"https://laws.e-gov.go.jp/law/{law_id}"
            title = picked["title"]
            law_num = picked.get("law_num") or ""

            docs.append({
                "source": "egov",
                "title": f"{title}（{law_num}）" if law_num else title,
                "url": url,
                "content": text,
                "extra": {"law_id": law_id, "law_num": law_num},
            })

            print(f"[eGov] fetched: {title} chars={len(text)}")
            time.sleep(delay_seconds)

            if len(docs) >= max_laws:
                return docs

    return docs
