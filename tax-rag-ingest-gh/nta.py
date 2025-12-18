import time
import re
import urllib.parse
from typing import Dict, List, Set, Tuple, Optional

import requests
from bs4 import BeautifulSoup

ALLOWED_HOST = "www.nta.go.jp"

def _compile_regex_list(patterns: Optional[List[str]]) -> List[re.Pattern]:
    if not patterns:
        return []
    out: List[re.Pattern] = []
    for p in patterns:
        if p:
            out.append(re.compile(p, re.IGNORECASE))
    return out

def _match_any(patterns: List[re.Pattern], text: str) -> bool:
    return any(p.search(text) for p in patterns)

def _is_allowed(url: str, allowed_prefixes: List[str]) -> bool:
    return any(url.startswith(p) for p in allowed_prefixes)

def _normalize_url(url: str, base_url: str) -> Optional[str]:
    try:
        url = urllib.parse.urljoin(base_url, url)
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return None
        if parsed.netloc and parsed.netloc != ALLOWED_HOST:
            return None
        parsed = parsed._replace(fragment="")  # drop fragments
        return parsed.geturl()
    except Exception:
        return None

def _extract_text_and_title(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "lxml")

    # noisy elements
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)

    main = (
        soup.find("main")
        or soup.find(id="main")
        or soup.find("article")
        or soup.find(class_="main")
        or soup.find(class_="mainContents")
    )
    target = main or soup.body or soup
    text = target.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return title, text

def crawl_nta(
    seeds: List[str],
    max_pages: int = 200,
    delay_seconds: float = 0.6,
    allowed_prefixes: Optional[List[str]] = None,
    exclude_url_regex: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    if not allowed_prefixes:
        # これが “基本通達” の大元。ここ配下だけを辿る前提
        allowed_prefixes = ["https://www.nta.go.jp/law/tsutatsu/kihon/"]

    exclude_patterns = _compile_regex_list(exclude_url_regex)

    seen: Set[str] = set()
    queue: List[str] = []

    for s in seeds:
        if _is_allowed(s, allowed_prefixes):
            queue.append(s)

    docs: List[Dict[str, str]] = []

    session = requests.Session()
    session.headers.update({"User-Agent": "tax-rag-mvp/0.2 (+https://example.invalid)"})

    while queue and len(seen) < max_pages:
        url = queue.pop(0)
        if url in seen:
            continue
        if exclude_patterns and _match_any(exclude_patterns, url):
            continue
        seen.add(url)

        try:
            r = session.get(url, timeout=30)
            if r.status_code != 200:
                continue

            ctype = r.headers.get("content-type", "")
            if "text/html" not in ctype:
                continue

            # 日本語ページで文字化け対策
            if (not r.encoding) or (r.encoding.lower() in ("iso-8859-1", "latin-1")):
                r.encoding = r.apparent_encoding or "utf-8"

            html = r.text
            title, text = _extract_text_and_title(html)

            docs.append({
                "source": "nta",
                "title": title or url,
                "url": url,
                "content": text,
                "extra": {},
            })

            # links
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                nurl = _normalize_url(a.get("href"), url)
                if not nurl:
                    continue
                if not _is_allowed(nurl, allowed_prefixes):
                    continue
                if exclude_patterns and _match_any(exclude_patterns, nurl):
                    continue
                if re.search(r"\.(pdf|zip|xls|xlsx|doc|docx)$", nurl, re.IGNORECASE):
                    continue
                if nurl not in seen:
                    queue.append(nurl)

        finally:
            time.sleep(delay_seconds)

    print(f"[NTA] crawled pages: {len(seen)} docs: {len(docs)}")
    return docs
