import time
import re
import urllib.parse
from typing import Dict, List, Set, Tuple, Optional

import requests
from bs4 import BeautifulSoup

ALLOWED_HOST = "www.nta.go.jp"

DEFAULT_ALLOWED_PREFIXES = [
    "https://www.nta.go.jp/law/tsutatsu/",
    "https://www.nta.go.jp/law/shitsugi/",
    "https://www.nta.go.jp/taxes/shiraberu/taxanswer/",
]

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
        # drop fragments
        parsed = parsed._replace(fragment="")
        return parsed.geturl()
    except Exception:
        return None

def _extract_text_and_title(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "lxml")

    # Remove noisy elements
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)

    # main text
    text = soup.get_text("\n", strip=True)
    # Basic cleanup
    text = re.sub(r"\n{3,}", "\n\n", text)
    return title, text

def crawl_nta(seeds: List[str], max_pages: int = 400, delay_seconds: float = 0.5, allowed_prefixes: Optional[List[str]] = None) -> List[Dict[str, str]]:
    allowed_prefixes = allowed_prefixes or DEFAULT_ALLOWED_PREFIXES

    seen: Set[str] = set()
    queue: List[str] = []

    for s in seeds:
        if _is_allowed(s, allowed_prefixes):
            queue.append(s)

    docs: List[Dict[str, str]] = []

    session = requests.Session()
    session.headers.update({"User-Agent": "tax-rag-mvp/0.1 (+https://example.invalid)"})

    while queue and len(seen) < max_pages:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        try:
            r = session.get(url, timeout=30)
            if r.status_code != 200:
                continue
            ctype = r.headers.get("content-type", "")
            if "text/html" not in ctype:
                continue

            html = r.text
            title, text = _extract_text_and_title(html)

            # Store doc
            docs.append({
                "source": "nta",
                "title": title or url,
                "url": url,
                "content": text,
                "extra": {},
            })

            # Extract links
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a.get("href")
                if not href:
                    continue
                nurl = _normalize_url(href, url)
                if not nurl:
                    continue
                if not _is_allowed(nurl, allowed_prefixes):
                    continue
                # Skip obvious binary
                if re.search(r"\.(pdf|zip|xls|xlsx|doc|docx)$", nurl, re.IGNORECASE):
                    continue
                if nurl not in seen:
                    queue.append(nurl)

        finally:
            time.sleep(delay_seconds)

    return docs
