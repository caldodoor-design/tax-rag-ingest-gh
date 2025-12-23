import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def _normalize_encoding(enc: Optional[str]) -> Optional[str]:
    if not enc:
        return None
    e = enc.strip().lower().replace("_", "").replace("-", "")
    if e in ("shiftjis", "sjis", "windows31j", "cp932"):
        return "cp932"  # 実務的にこれが一番事故りにくい
    if e in ("eucjp", "euc-jp"):
        return "euc-jp"
    if e in ("utf8", "utf-8"):
        return "utf-8"
    return enc.strip()


def _sniff_charset_from_html_head(raw: bytes) -> Optional[str]:
    # HTMLの先頭数KBにある meta charset はASCIIで書かれてるので、バイト列に対して検索できる
    head = raw[:4096]

    m = re.search(br"<meta[^>]*charset=['\"]?\s*([a-zA-Z0-9_\-]+)\s*['\"]?", head, re.I)
    if m:
        try:
            return _normalize_encoding(m.group(1).decode("ascii", errors="ignore"))
        except Exception:
            pass

    m = re.search(br"charset\s*=\s*([a-zA-Z0-9_\-]+)", head, re.I)
    if m:
        try:
            return _normalize_encoding(m.group(1).decode("ascii", errors="ignore"))
        except Exception:
            pass

    return None


def _decode_html_bytes(raw: bytes, header_content_type: str, fallback: str = "cp932") -> str:
    # 1) HTTPヘッダの charset
    header_enc = None
    if header_content_type:
        m = re.search(r"charset\s*=\s*([^;]+)", header_content_type, re.I)
        if m:
            header_enc = _normalize_encoding(m.group(1))

    # 2) HTML meta charset
    meta_enc = _sniff_charset_from_html_head(raw)

    # 3) 候補の優先順（KFSはcp932が多い想定でフォールバック）
    candidates = []
    for e in (meta_enc, header_enc, fallback, "euc-jp", "utf-8"):
        if e and e not in candidates:
            candidates.append(e)

    # 4) デコード試行
    for enc in candidates:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue

    # 最後は壊れてもいいから可視化（置換）
    return raw.decode(candidates[0] if candidates else "utf-8", errors="replace")


# --- Heuristics (ここがキモ) ---
# これ未満のページは「ホーム/索引/目次」っぽいので捨てる
MIN_CONTENT_CHARS_DEFAULT = 2000

# 裁決本文にありがちな語（どれか1つでも含まれてれば短くても通す）
CASE_KEYWORDS_DEFAULT = [
    "裁決年月日",
    "裁決要旨",
    "主文",
    "理由",
    "請求の趣旨",
    "請求人",
    "処分庁",
    "争点",
    "判断",
]

# タイトルや本文にこれが強く出るページは捨てる
EXCLUDE_TITLES = {"ホーム"}
EXCLUDE_URL_PATTERNS = [
    r"/service/index\.html$",
    r"/service/JP/index\.html$",
]


def _get_soup(url: str, timeout: int = 25) -> Optional[BeautifulSoup]:
    try:
        res = requests.get(url, headers=HEADERS, timeout=timeout)
        res.raise_for_status()
        res.encoding = res.apparent_encoding or "utf-8"
        return BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"[KFS] fetch failed: {url} / {e}")
        return None


def _clean_text(area: BeautifulSoup) -> str:
    # ノイズ除去
    for tag in area.select(
        "nav, header, footer, .header, .footer, .breadcrumb, "
        "script, style, .btn-area, .page-top, .gnav, .menu, .global-nav"
    ):
        tag.decompose()
    return area.get_text(separator="\n", strip=True)


def _pick_title(soup: BeautifulSoup, fallback: str) -> str:
    # なるべくページの見出しを取る
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)[:120]
    t = soup.find("title")
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)[:120]
    return fallback[:120]


def _looks_like_index_page(url: str, title: str, text: str) -> bool:
    if title.strip() in EXCLUDE_TITLES:
        return True
    for pat in EXCLUDE_URL_PATTERNS:
        if re.search(pat, url):
            return True
    # 本文がほぼ無い + リンクっぽい行が多い → 目次/索引の可能性
    lines = [ln for ln in text.split("\n") if ln.strip()]
    if len(lines) <= 8 and len(text) < 800:
        return True
    return False


def _passes_case_heuristics(
    url: str,
    title: str,
    text: str,
    min_chars: int,
    require_any_keywords: List[str],
) -> bool:
    if not text:
        return False
    if _looks_like_index_page(url, title, text):
        return False

    # キーワードが含まれていれば短くても許す
    if any(k in text for k in require_any_keywords):
        return True

    # それ以外は長さで判定
    return len(text) >= min_chars


def _extract_case_text_and_title(url: str) -> (str, str):
    soup = _get_soup(url)
    if not soup:
        return "", ""

    content_area = soup.find("div", id="contents") or soup.find("main") or soup.find("body")
    if not content_area:
        return "", ""

    text = _clean_text(content_area)
    title = _pick_title(soup, fallback=(text.split("\n", 1)[0] if text else url))
    return text, title


def collect_kfs_saiketsu(
    start_url: str = "https://www.kfs.go.jp/service/JP/index.html",
    delay_seconds: float = 1.2,
    max_cases: int = 0,  # 0 = 無制限
    include_youshi: bool = False,  # Trueにすると「要旨」も入れる（重複増えがち）
    min_content_chars: int = MIN_CONTENT_CHARS_DEFAULT,
    require_any_keywords: List[str] = None,
) -> List[Dict]:
    """
    KFS 公表裁決事例（JP）を収集して docs を返す（ingest.py で使える形式）
    - Phase1: start_url から「裁決事例集」リンク（年度/号リスト）を集める
    - Phase2: 各リストから「裁決事例」リンクを集める
    - Phase3: 本文取得。ただし「ホーム/索引/目次」っぽいページは捨てる
    """
    if require_any_keywords is None:
        require_any_keywords = CASE_KEYWORDS_DEFAULT

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
        if "裁決事例集" in text:
            u = urljoin(start_url, href)
            list_pages.append(u)

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

            # 明らかなホーム/目次系はURL段階で落とす
            if any(re.search(pat, u) for pat in EXCLUDE_URL_PATTERNS):
                continue

            if u not in case_urls:
                case_urls.append(u)

        time.sleep(delay_seconds)

    if max_cases and max_cases > 0:
        case_urls = case_urls[:max_cases]

    print(f"[KFS] case pages found: {len(case_urls)}")

    # -------------------------
    # Phase 3: 本文取得 → docs化（薄いページは捨てる）
    # -------------------------
    docs: List[Dict] = []
    for idx, url in enumerate(case_urls):
        print(f"[KFS] ({idx+1}/{len(case_urls)}) fetching: {url}")
        content, title = _extract_case_text_and_title(url)

        if not _passes_case_heuristics(
            url=url,
            title=title or url,
            text=content or "",
            min_chars=min_content_chars,
            require_any_keywords=require_any_keywords,
        ):
            # 薄いページ/索引/ホームはここで落ちる
            print(f"[KFS] skip (index/too short): {url} chars={len(content)} title={title}")
            time.sleep(delay_seconds)
            continue

        docs.append(
            {
                "source": "kfs",
                "title": title or (content.split("\n", 1)[0][:120] if content else url),
                "url": url,
                "content": content,
                "extra": {"kfs_kind": "saiketsu"},
            }
        )

        time.sleep(delay_seconds)

    print(f"[KFS] done. docs={len(docs)}")
    return docs
