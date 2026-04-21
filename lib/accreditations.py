"""UK regulatory/accreditation marker detection."""

import re

# (marker, min_context) — short markers need word boundary matching
MARKERS = [
    # Fire & Security
    ("BAFE", True), ("FIRAS", True), ("NSI Gold", False), ("NSI approved", False),
    ("SSAIB", True),
    # Electrical
    ("NICEIC", True), ("NAPIT", True), ("ECA approved", False), ("ECA registered", False),
    # Gas & Heating
    ("Gas Safe", False), ("HETAS", True), ("OFTEC", True),
    # Care & Education
    ("OFSTED", True), ("Ofsted", True), ("CQC registered", False), ("CQC rated", False),
    # Financial
    ("FCA regulated", False), ("FCA authorised", False),
    # Automotive
    ("MOT testing", False), ("MOT station", False), ("MOT centre", False),
    ("DVSA", True),
    # Pest Control
    ("BPCA", True),
    # Construction & Trade
    ("CITB", True), ("CSCS", True), ("CPCS", True), ("IPAF", True), ("PASMA", True),
    ("Constructionline", False), ("SafeContractor", False), ("CHAS", True),
    ("SMAS", True), ("TrustMark", False),
    # Renewables
    ("MCS certified", False), ("MCS accredited", False),
    # ISO
    ("ISO 9001", False), ("ISO 14001", False), ("ISO 45001", False),
    # Standards
    ("BS 5839", False), ("BS 5306", False),
    # Professional bodies
    ("IOSH", True), ("NEBOSH", True),
    ("IMechE", True), ("RICS", True),
    ("Arboricultural Association", False),
    # Trees
    ("LANTRA", True),
    # Fire Industry
    ("Fire Industry Association", False),
]


def detect_accreditations(text: str) -> list[str]:
    """Scan text for UK regulatory/accreditation markers with strict matching."""
    if not text:
        return []
    found = []
    for marker, strict_boundary in MARKERS:
        if strict_boundary:
            # Require word boundaries for short acronyms to avoid false positives
            pattern = r"\b" + re.escape(marker) + r"\b"
        else:
            pattern = re.escape(marker)
        if re.search(pattern, text, re.IGNORECASE):
            # Extract the base marker name (before qualifiers)
            base = marker.split()[0] if " " in marker else marker
            if base not in found:
                found.append(base)
    return found
