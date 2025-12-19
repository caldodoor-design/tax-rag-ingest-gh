import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
}


def _get_soup(url: str, timeout: int = 20) -> Optional[BeautifulSoup]:
    try:
        res = requests.get(url, headers=HEADERS, timeout=timeout)
        res.encoding = res.apparent_encoding or "utf-8"
        return BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"[KFS] fetch failed: {url} / {e}")
        return None


def _extract_case_text(url: str) -> str:
    soup = _get_soup(url)
    if not soup:
        return ""

    content_area = soup.find("div", id="contents") or soup.find("main") or soup.find("body")
    if not content_area:
        return ""

    # ノイズ除去（Colab版の考え方を踏襲）
    for tag in content_area.select(
        "nav, .header, .footer, .breadcrumb, script, style, .btn-area, .page-top"
    ):
        tag.decompose()

    text = content_area.get_text(separator="\n", strip=True)
    return text


def collect_kfs_saiketsu(
    start_url: str = "https://www.kfs.go.jp/service/JP/index.html",
    delay_seconds: float = 1.2,
    max_cases: int = 0,  # 0 = 無制限
    include_youshi: bool = False,  # Trueにすると「要旨」も入れる（重複増えがち）
) -> List[Dict]:
    """
    KFS 公表裁決事例（JP）を収集して docs を返す（ingest.py でそのまま使える形式）
    - Phase1: start_url から「裁決事例集」リンク（年度/号リスト）を集める
    - Phase2: 各リストから「裁決事例」リンクを集め、本文を取得
    """
    print(f"[KFS] start: {start_url}")

    # -------------------------
    # Phase 1: リストページ収集
    # -------------------------
    soup = _get_soup(start_url)
    if not soup:
        return []

    list_pages: List[str] = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        # 「裁決事例集」リンクを拾う（公式ページの導線に合わせる） :contentReference[oaicite:1]{index=1}
        if "裁決事例集" in text:
            list_pages.append(urljoin(start_url, href))

    list_pages = sorted(list(set(list_pages)), reverse=True)
    print(f"[KFS] list pages found: {len(list_pages)}")

    # -------------------------
    # Phase 2: 事例リンク抽出
    # -------------------------
    case_urls: List[str] = []
    for i, list_url in enumerate(list_pages):
        print(f"[KFS] scanning list {i+1}/{len(list_pages)}: {list_url}")
        l_soup = _get_soup(list_url)
        if not l_soup:
            time.sleep(delay_seconds)
            continue

        for a in l_soup.find_all("a", href=True):
            t = a.get_text(strip=True)
            if "裁決事例" not in t:
                continue
            if (not include_youshi) and ("要旨" in t):
                continue

            u = urljoin(list_url, a["href"])
            if not (u.endswith(".html") or u.endswith(".htm")):
                continue
            if u not in case_urls:
                case_urls.append(u)

        time.sleep(delay_seconds)

    if max_cases and max_cases > 0:
        case_urls = case_urls[:max_cases]

    print(f"[KFS] case pages found: {len(case_urls)}")

    # -------------------------
    # Phase 3: 本文取得 → docs化
    # -------------------------
    docs: List[Dict] = []
    for idx, url in enumerate(case_urls):
        print(f"[KFS] ({idx+1}/{len(case_urls)}) fetching: {url}")
        content = _extract_case_text(url)
        if not content:
            time.sleep(delay_seconds)
            continue

        title = content.split("\n", 1)[0][:120] if content else url

        docs.append(
            {
                "source": "kfs",
                "title": title,
                "url": url,
                "content": content,
                "extra": {"kfs_kind": "saiketsu"},
            }
        )

        time.sleep(delay_seconds)

    print(f"[KFS] done. docs={len(docs)}")
    return docs
