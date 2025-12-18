import requests
from lxml import etree
from typing import Dict, List, Tuple

# e-Gov 法令API Version1 (XML)
# Docs: https://laws.e-gov.go.jp/docs/ ... (law-data documentation alpha)
BASE_V1 = "https://elaws.e-gov.go.jp/api/1"

def _get_xml(url: str, timeout: int = 30) -> etree._Element:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "tax-rag-mvp/0.1"})
    r.raise_for_status()
    parser = etree.XMLParser(recover=True)
    return etree.fromstring(r.content, parser=parser)

def fetch_law_list(category: int = 1) -> List[Dict[str, str]]:
    """
    category: 1 is commonly used in examples (all current laws).
    Returns list of {law_id, law_name, law_no}
    """
    root = _get_xml(f"{BASE_V1}/lawlists/{category}")
    out = []

    # Attempt to find LawList elements regardless of exact nesting
    for law in root.xpath(".//*[local-name()='LawList']"):
        law_id = law.findtext(".//*[local-name()='LawID']") or law.findtext(".//*[local-name()='LawId']")
        law_name = law.findtext(".//*[local-name()='LawName']")
        law_no = law.findtext(".//*[local-name()='LawNo']") or law.findtext(".//*[local-name()='LawNum']")
        if law_id and law_name:
            out.append({"law_id": law_id.strip(), "law_name": law_name.strip(), "law_no": (law_no or "").strip()})

    # Some responses may use LawInfo nodes
    if not out:
        for law in root.xpath(".//*[local-name()='LawInfo']"):
            law_id = law.findtext(".//*[local-name()='LawID']") or law.findtext(".//*[local-name()='LawId']")
            law_name = law.findtext(".//*[local-name()='LawName']")
            law_no = law.findtext(".//*[local-name()='LawNo']") or law.findtext(".//*[local-name()='LawNum']")
            if law_id and law_name:
                out.append({"law_id": law_id.strip(), "law_name": law_name.strip(), "law_no": (law_no or "").strip()})
    return out

def fetch_law_text(law_id: str) -> Tuple[str, str]:
    """
    Returns (title, plain_text) for a given law_id.
    """
    root = _get_xml(f"{BASE_V1}/lawdata/{law_id}")
    title = root.findtext(".//*[local-name()='LawName']") or ""

    # Extract text from the main law body if present
    body_nodes = (
        root.xpath(".//*[local-name()='LawFullText']")
        or root.xpath(".//*[local-name()='LawBody']")
        or [root]
    )

    texts = []
    for n in body_nodes:
        texts.append(" ".join(t.strip() for t in n.itertext() if t and t.strip()))
    full_text = "\n".join([t for t in texts if t]).strip()
    return title.strip(), full_text

def collect_laws_by_keywords(keywords: List[str], max_laws: int = 30, category: int = 1) -> List[Dict[str, str]]:
    laws = fetch_law_list(category=category)
    hits = []
    for law in laws:
        name = law["law_name"]
        if any(k in name for k in keywords):
            hits.append(law)
        if len(hits) >= max_laws:
            break

    out_docs = []
    for law in hits:
        title, text = fetch_law_text(law["law_id"])
        url = f"https://laws.e-gov.go.jp/law/{law['law_id']}"
        out_docs.append({
            "source": "egov",
            "title": title or law["law_name"],
            "url": url,
            "content": text,
            "extra": {"law_id": law["law_id"], "law_no": law.get("law_no", "")},
        })
    return out_docs
