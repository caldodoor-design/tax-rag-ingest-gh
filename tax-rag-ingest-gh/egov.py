import time
from typing import Dict, List, Optional, Set, Tuple

import requests

BASE_V2 = "https://laws.e-gov.go.jp/api/2"

# ====== your policy ======
EGOV_TITLE_ALLOWLIST: Set[str] = {
    "法人税法", "所得税法", "消費税法", "相続税法", "贈与税法",
    "国税通則法", "国税徴収法", "租税特別措置法", "印紙税法",
    "地方税法", "税理士法", "会社法", "会社法施行規則", "会社計算規則",
    "商業登記法", "商業登記規則", "労働基準法", "健康保険法", "厚生年金保険法",
    "雇用保険法", "労働保険の保険料の徴収等に関する法律",
    "減価償却資産の耐用年数等に関する省令",
}

EGOV_PREFIX_ALLOW: List[str] = [
    "法人税法", "所得税法", "消費税法", "相続税法", "贈与税法",
    "国税通則法", "国税徴収法", "租税特別措置法", "印紙税法",
    "地方税法", "税理士法", "会社法", "労働基準法", "健康保険法",
    "厚生年金保険法", "雇用保険法", "労働保険の保険料の徴収等に関する法律",
]

EGOV_EXCLUDE_PHRASES: List[str] = [
    "の一部を改正する法律", "等の一部を改正する法律",
    "整備法", "改正法", "廃止",
    "特別会計", "交付金", "特例公債", "地方交付税",
]
# ========================


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


def search_laws_by_title(session: requests.Session, law_title: str, limit: int = 50) -> List[Dict]:
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
    return _get_json(
        session,
        f"{BASE_V2}/law_data/{law_id}",
        params={
            "response_format": "json",
            "law_full_text_format": "json",
        },
    )


def _is_excluded(title: str) -> bool:
    return any(p in title for p in EGOV_EXCLUDE_PHRASES)


def _prefix_allowed(title: str) -> bool:
    return any(title.startswith(p) for p in EGOV_PREFIX_ALLOW)


def _pick_best_for_keyword(keyword: str, items: List[Dict]) -> Optional[Tuple[str, str, str]]:
    """
    returns (law_id, title, law_num)
    """
    candidates = []
    for item in items:
        law_info = item.get("law_info", {}) or {}
        rev_info = item.get("revision_info", {}) or {}

        law_id = law_info.get("law_id")
        title = (rev_info.get("law_title") or "").strip()
        law_num = (law_info.get("law_num") or "").strip()
        if not law_id or not title:
            continue

        if _is_excluded(title):
            continue

        # primary: exact match to what user asked
        if title == keyword:
            score = 10_000

        # secondary: exact allowlist
        elif title in EGOV_TITLE_ALLOWLIST:
            score = 9_000

        # fallback: prefix allow (only if keyword is one of the prefixes)
        elif keyword in EGOV_PREFIX_ALLOW and title.startswith(keyword) and _prefix_allowed(title):
            # prefer shorter titles (avoid long "…の臨時特例…" etc)
            score = 8_000 - min(len(title), 500)

        else:
            continue

        candidates.append((score, law_id, title, law_num))

    if not candidates:
        return None

    candidates.sort(reverse=True, key=lambda x: x[0])
    _, law_id, title, law_num = candidates[0]
    return law_id, title, law_num


def collect_laws_by_keywords(
    keywords: List[str],
    max_laws: int = 30,
    per_keyword_limit: int = 50,
    delay_seconds: float = 0.3,
    category: Optional[int] = None,
    **_ignored,
) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "tax-rag-mvp/0.1 (+https://example.invalid)"})

    docs: List[Dict[str, str]] = []
    seen_law_ids: Set[str] = set()

    for kw in keywords:
        print(f"[eGov] searching: {kw}")
        items = search_laws_by_title(session, law_title=kw, limit=per_keyword_limit)

        picked = _pick_best_for_keyword(kw, items)
        if not picked:
            print(f"[eGov] NOT FOUND (or excluded): {kw}")
            continue

        law_id, title, law_num = picked
        if law_id in seen_law_ids:
            continue
        seen_law_ids.add(law_id)

        data = fetch_law_full_text(session, law_id=law_id)
        if not data:
            print(f"[eGov] failed to fetch law_data: {kw} id={law_id}")
            continue

        text = _extract_text(data.get("law_full_text")).strip()
        if not text:
            print(f"[eGov] empty text: {kw} id={law_id}")
            continue

        url = f"https://laws.e-gov.go.jp/law/{law_id}"

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
            break

    return docs
