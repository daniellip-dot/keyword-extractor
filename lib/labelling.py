"""Generate sector_label and confidence from signals."""


SCHEMA_BUSINESS_TYPES = {
    "AutoRepair": "Auto Repair",
    "AutoDealer": "Auto Dealer",
    "ChildCare": "Child Care",
    "Dentist": "Dental Practice",
    "ElectricalContractor": "Electrical Contractor",
    "GeneralContractor": "General Contractor",
    "HVACBusiness": "HVAC Services",
    "Plumber": "Plumbing Services",
    "RoofingContractor": "Roofing Contractor",
    "ProfessionalService": "Professional Services",
    "FinancialService": "Financial Services",
    "InsuranceAgency": "Insurance",
    "RealEstateAgent": "Estate Agent",
    "LegalService": "Legal Services",
    "AccountingService": "Accountancy",
    "EducationalOrganization": "Education",
    "School": "School",
    "Restaurant": "Restaurant",
    "BarOrPub": "Pub",
    "Hotel": "Hotel",
    "Store": "Retail",
    "MedicalBusiness": "Healthcare",
    "Pharmacy": "Pharmacy",
    "VeterinaryCare": "Veterinary Practice",
    "LocalBusiness": None,  # too generic
    "Organization": None,
    "WebSite": None,
    "WebPage": None,
}

ACCRED_SECTORS = {
    "Gas": "Gas Engineering",
    "NICEIC": "Electrical Services",
    "NAPIT": "Electrical Services",
    "ECA": "Electrical Services",
    "BAFE": "Fire Safety",
    "FIRAS": "Fire Protection",
    "NSI": "Security Systems",
    "SSAIB": "Security Systems",
    "Ofsted": "Education & Childcare",
    "OFSTED": "Education & Childcare",
    "CQC": "Healthcare",
    "FCA": "Financial Services",
    "MOT": "Vehicle Repair & MOT",
    "DVSA": "Vehicle Testing",
    "BPCA": "Pest Control",
    "HETAS": "Heating & Stoves",
    "OFTEC": "Oil Heating",
    "MCS": "Renewable Energy",
    "TrustMark": "Building & Trade",
    "CITB": "Construction",
    "CSCS": "Construction",
}


def generate_label(
    schema_type: str,
    service_phrases: list[str],
    top_keywords: list[str],
    accreditations: list[str],
) -> tuple[str, str]:
    """Return (sector_label, confidence)."""

    # 1. Schema.org specific business type (highest signal)
    if schema_type:
        mapped = SCHEMA_BUSINESS_TYPES.get(schema_type)
        if mapped:  # None means generic, skip
            return mapped, "HIGH"

    # 2. Strong service phrases first (more descriptive than accreditations)
    if service_phrases and len(service_phrases) >= 2:
        label = service_phrases[0].title()
        # If accreditation reinforces the phrase, boost confidence
        if accreditations:
            return label, "HIGH"
        return label, "MEDIUM"

    # 3. Accreditation as label (only if no good phrases)
    if accreditations:
        for acc in accreditations:
            if acc in ACCRED_SECTORS:
                return ACCRED_SECTORS[acc], "HIGH"

    # 4. Single service phrase
    if service_phrases:
        return service_phrases[0].title(), "MEDIUM"

    # 5. Combine top keywords
    if len(top_keywords) >= 2:
        label = " ".join(top_keywords[:3]).title()
        if len(label) > 40:
            label = " ".join(top_keywords[:2]).title()
        return label, "LOW"

    if len(top_keywords) == 1:
        return top_keywords[0].title(), "LOW"

    return "UNKNOWN", "NONE"
