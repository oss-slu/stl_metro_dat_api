from __future__ import annotations
import re
import json
import gc
from typing import Dict, Any, List, Iterable, Optional


DATE_PATTERNS = [
    r'\b\d{4}-\d{2}-\d{2}\b', # ISO dates: yyyy-mm-dd
    r'\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b', # dd/mm/yyyy or dd-mm-yyyy or d/m/yy

    # month names (month dd, yyyy or mon dd yyyy)
    r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|'
    r'May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|'
    r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2}(?:,\s*\d{4})?\b', 
]


# name pattern
NAME_PATTERN = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+){0,2})\b'

# common false-positives
COMMON_NON_NAMES = {
    "January","February","March","April","May","June","July","August","September","October","November","December",
    "Jan","Feb","Mar","Apr","Jun","Jul","Aug","Sep","Oct","Nov","Dec",
    "The","A","An","In","On","At","For","And","But","Or","By","To","From","With"
}

def clean_text(text: str) -> str:
    t = re.sub(r'[\r\n\t]+', ' ', text) # replace control chars with spaces
    t = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+', ' ', t) # remove non-printable chars
    t = re.sub(r'\s+', ' ', t) # trim whitespace
    return t.strip()


def extract_entities(text: str) -> Dict[str, List[str]]:
    dates = []
    for pat in DATE_PATTERNS:
        for m in re.finditer(pat, text):
            v = m.group(0).strip()
            if v not in dates:
                dates.append(v)

    # filter out months and common words
    raw_names = []
    for m in re.finditer(NAME_PATTERN, text):
        candidate = m.group(1).strip()
        if len(candidate) < 2:
            continue
        if candidate in COMMON_NON_NAMES:
            continue
        if re.fullmatch(r'\d{2,4}', candidate): # check if it looks like a year
            continue
        if candidate not in raw_names:
            raw_names.append(candidate)

    return {
        "dates": dates,
        "names": raw_names
    }


def build_payload(pdf_id: str, entities: Dict[str, Any], snippet: Optional[str] = None) -> bytes:
    payload = {
        "pdf_id": pdf_id,
        "entities": entities,
        "snippet": (snippet or "")[:200],
        "schema_version": 1
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def process_and_send(pdf_id: str, raw_bytes: Any, producer: Any, topic: str = "pdf-processed-topic") -> Dict[str, Any]:
    is_bytearray = isinstance(raw_bytes, bytearray)
    is_bytes = isinstance(raw_bytes, (bytes, bytearray))
    is_str = isinstance(raw_bytes, str)

    if is_bytearray:
        text = raw_bytes.decode("utf-8", errors="ignore")
    elif is_bytes:
        text = raw_bytes.decode("utf-8", errors="ignore")
    elif is_str:
        text = raw_bytes
    else:
        text = str(raw_bytes)

    cleaned = clean_text(text)
    entities = extract_entities(cleaned)
    snippet = cleaned[:300]

    payload_bytes = build_payload(pdf_id, entities, snippet)

    send_successful = False
    try:
        if hasattr(producer, "send"):
            res = producer.send(topic, payload_bytes)
        elif hasattr(producer, "produce"):
            res = producer.produce(topic, payload_bytes)
        elif callable(producer):
            res = producer(topic, payload_bytes)
        else:
            raise AttributeError("Producer does not have callable interface")
        send_successful = True

    finally:
        # clear raw bytes from memory
        try:
            if is_bytearray:
                for i in range(len(raw_bytes)):
                    raw_bytes[i] = 0
            else:
                del text
                del cleaned
                gc.collect()
        except Exception:
            pass

    return {
        "pdf_id": pdf_id,
        "entities": entities,
        "snippet": snippet,
        "sent": bool(send_successful)
    }
