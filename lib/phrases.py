"""Service phrase pattern matching."""

import re
from collections import Counter

PATTERNS = [
    r"\b\w+ing\s+\w+\b",
    r"\b\w+\s+services?\b",
    r"\b\w+\s+installation\b",
    r"\b\w+\s+repairs?\b",
    r"\b\w+\s+maintenance\b",
    r"\b\w+\s+supply\b",
    r"\b\w+\s+specialists?\b",
    r"\b\w+\s+contractors?\b",
    r"\b\w+\s+consultants?\b",
    r"\b\w+\s+engineers?\b",
    r"\b\w+\s+testing\b",
    r"\b\w+\s+certification\b",
    r"\b\w+\s+assessments?\b",
    r"\bmanufacturers?\s+of\s+\w+\b",
    r"\bspecialists?\s+in\s+\w+\b",
]

PHRASE_STOPS = {
    "the", "and", "for", "our", "your", "this", "that", "with",
    "are", "was", "has", "had", "have", "been", "being", "having",
    "all", "any", "each", "every", "more", "most", "other",
    "some", "such", "than", "too", "very", "just", "also",
    "using", "including", "getting", "making", "looking",
    "coming", "going", "finding", "keeping", "taking",
}


def extract_service_phrases(weighted_text: str, top_n: int = 10) -> list[str]:
    """Extract multi-word service/activity phrases."""
    if not weighted_text:
        return []

    text_lower = weighted_text.lower()
    found = Counter()

    for pattern in PATTERNS:
        matches = re.findall(pattern, text_lower)
        for match in matches:
            words = match.split()
            if any(w in PHRASE_STOPS for w in words):
                continue
            if len(match) < 6:
                continue
            found[match.strip()] += 1

    return [phrase for phrase, _ in found.most_common(top_n)]
