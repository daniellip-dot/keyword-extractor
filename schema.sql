CREATE TABLE IF NOT EXISTS keyword_extraction (
    domain              TEXT PRIMARY KEY,
    company_number      TEXT,
    company_name        TEXT,
    scrape_status       TEXT,
    scrape_error        TEXT,
    page_title          TEXT,
    meta_description    TEXT,
    h1_text             TEXT,
    headings            TEXT,
    schema_type         TEXT,
    og_type             TEXT,
    top_keywords        TEXT,
    service_phrases     TEXT,
    accreditations      TEXT,
    sector_label        TEXT,
    confidence          TEXT,
    raw_text_sample     TEXT,
    processed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_confidence ON keyword_extraction(confidence);
CREATE INDEX IF NOT EXISTS idx_sector ON keyword_extraction(sector_label);
