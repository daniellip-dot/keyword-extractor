"""Extract signal layers from parsed HTML."""

import json
import re
from bs4 import BeautifulSoup


def extract_signals(soup: BeautifulSoup) -> dict:
    """Extract all signal layers from parsed page."""
    signals = {}

    # Title
    title_tag = soup.find("title")
    signals["title"] = title_tag.get_text(strip=True) if title_tag else ""

    # Meta description
    meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    signals["meta_description"] = meta_desc.get("content", "") if meta_desc else ""

    # Meta keywords
    meta_kw = soup.find("meta", attrs={"name": re.compile(r"keywords", re.I)})
    signals["meta_keywords"] = meta_kw.get("content", "") if meta_kw else ""

    # H1
    h1s = soup.find_all("h1")
    signals["h1"] = " ".join(h.get_text(strip=True) for h in h1s)

    # H2 + H3
    h2h3 = soup.find_all(["h2", "h3"])
    signals["h2_h3"] = " | ".join(h.get_text(strip=True) for h in h2h3[:20])

    # OG type
    og = soup.find("meta", attrs={"property": "og:type"})
    signals["og_type"] = og.get("content", "") if og else ""

    # JSON-LD schema
    signals["schema_type"] = ""
    signals["schema_desc"] = ""
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                st = data.get("@type", "")
                if isinstance(st, list):
                    st = st[0] if st else ""
                if st:
                    signals["schema_type"] = st
                sd = data.get("description", "")
                if sd:
                    signals["schema_desc"] = sd[:500]
                break
        except (json.JSONDecodeError, TypeError):
            continue

    # Body text (first 3000 chars)
    body = soup.find("body")
    if body:
        text = body.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        signals["body_text"] = text[:3000]
    else:
        signals["body_text"] = ""

    return signals


def build_weighted_text(signals: dict) -> str:
    """Build weighted text for keyword extraction."""
    parts = []
    weights = {
        "title": 5,
        "meta_description": 5,
        "meta_keywords": 3,
        "schema_desc": 4,
        "h1": 3,
        "h2_h3": 2,
        "body_text": 1,
    }
    for key, weight in weights.items():
        text = signals.get(key, "")
        if text:
            parts.extend([text] * weight)
    return " ".join(parts)
