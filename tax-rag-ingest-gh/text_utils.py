import re
from typing import List

def clean_text(text: str) -> str:
    # Normalize whitespace but keep paragraph breaks
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Remove excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Trim lines
    text = "\n".join(line.strip() for line in text.split("\n"))
    # Collapse multiple spaces
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()

def _split_long_para(para: str, max_chars: int) -> List[str]:
    # Split by Japanese sentence end if possible
    if len(para) <= max_chars:
        return [para]
    parts = re.split(r"(。|\.|！|!|？|\?)", para)
    # Recombine keeping punctuation
    sents = []
    buf = ""
    for i in range(0, len(parts), 2):
        seg = parts[i]
        punc = parts[i+1] if i+1 < len(parts) else ""
        piece = (seg + punc).strip()
        if not piece:
            continue
        if len(buf) + len(piece) + 1 <= max_chars:
            buf += piece
        else:
            if buf:
                sents.append(buf)
            buf = piece
    if buf:
        sents.append(buf)

    # If still too long (no punctuation), hard split
    out = []
    for s in sents:
        if len(s) <= max_chars:
            out.append(s)
        else:
            for j in range(0, len(s), max_chars):
                out.append(s[j:j+max_chars])
    return out

def chunk_text(text: str, max_chars: int = 1200, overlap_chars: int = 200) -> List[str]:
    text = clean_text(text)
    if not text:
        return []

    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    # Expand long paragraphs
    expanded: List[str] = []
    for p in paras:
        expanded.extend(_split_long_para(p, max_chars))

    chunks: List[str] = []
    buf = ""
    for p in expanded:
        if not buf:
            buf = p
            continue
        if len(buf) + 2 + len(p) <= max_chars:
            buf = buf + "\n\n" + p
        else:
            chunks.append(buf)
            # overlap: carry tail of previous chunk
            tail = buf[-overlap_chars:] if overlap_chars > 0 and len(buf) > overlap_chars else ""
            buf = (tail + "\n\n" + p).strip() if tail else p

    if buf:
        chunks.append(buf)

    # Final cleanup
    return [clean_text(c) for c in chunks if clean_text(c)]
