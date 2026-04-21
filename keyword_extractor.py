#!/usr/bin/env python3
"""
keyword_extractor.py — UK company website keyword extraction for sector labelling.
Pure scraping + TF-IDF + n-gram extraction. No LLM required.
"""

import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from lib.scraper import fetch_page, parse_html
from lib.signals import extract_signals, build_weighted_text
from lib.keywords import extract_keywords
from lib.phrases import extract_service_phrases
from lib.accreditations import detect_accreditations
from lib.labelling import generate_label

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "./data/keywords.db")
WORKERS = int(os.getenv("WORKERS", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_db():
    """Create DB and tables."""
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA_PATH.read_text())
    conn.close()


def get_processed_domains() -> set:
    """Get domains already processed successfully."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT domain FROM keyword_extraction WHERE scrape_status='success'"
        ).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()
    finally:
        conn.close()


def save_result(result: dict):
    """Save one result to SQLite."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cols = [
        "domain", "company_number", "company_name", "scrape_status",
        "scrape_error", "page_title", "meta_description", "h1_text",
        "headings", "schema_type", "og_type", "top_keywords",
        "service_phrases", "accreditations", "sector_label",
        "confidence", "raw_text_sample",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    values = [result.get(c, "") for c in cols]
    conn.execute(
        f"INSERT OR REPLACE INTO keyword_extraction ({col_names}) VALUES ({placeholders})",
        values,
    )
    conn.commit()
    conn.close()


def process_one(row: dict) -> dict:
    """Process a single company domain."""
    domain = (row.get("domain") or "").strip()
    result = {
        "domain": domain,
        "company_number": row.get("company_number", ""),
        "company_name": row.get("company_name", ""),
        "scrape_status": "failed",
        "scrape_error": "",
        "page_title": "",
        "meta_description": "",
        "h1_text": "",
        "headings": "",
        "schema_type": "",
        "og_type": "",
        "top_keywords": "",
        "service_phrases": "",
        "accreditations": "",
        "sector_label": "UNKNOWN",
        "confidence": "NONE",
        "raw_text_sample": "",
    }

    if not domain:
        result["scrape_error"] = "empty_domain"
        return result

    # Step 1: Fetch
    html, final_url, status = fetch_page(domain, timeout=REQUEST_TIMEOUT)

    if status == "blocked":
        result["scrape_status"] = "blocked"
        result["scrape_error"] = "blocked"
        return result

    if status.startswith("failed"):
        result["scrape_status"] = "failed"
        result["scrape_error"] = status
        return result

    result["scrape_status"] = "success"

    # Step 2: Parse
    soup = parse_html(html)

    # Step 3: Extract signals
    signals = extract_signals(soup)
    result["page_title"] = signals.get("title", "")[:500]
    result["meta_description"] = signals.get("meta_description", "")[:1000]
    result["h1_text"] = signals.get("h1", "")[:500]
    result["headings"] = signals.get("h2_h3", "")[:2000]
    result["schema_type"] = signals.get("schema_type", "")
    result["og_type"] = signals.get("og_type", "")
    result["raw_text_sample"] = signals.get("body_text", "")[:500]

    # Step 4: Build weighted text
    weighted = build_weighted_text(signals)

    # Step 5: Keywords
    kws = extract_keywords(weighted)
    result["top_keywords"] = ", ".join(kws)

    # Step 6: Service phrases
    phrases = extract_service_phrases(weighted)
    result["service_phrases"] = ", ".join(phrases)

    # Step 7: Accreditations
    accreds = detect_accreditations(weighted)
    result["accreditations"] = ", ".join(accreds)

    # Step 8+9: Label + confidence
    label, conf = generate_label(
        signals.get("schema_type", ""),
        phrases,
        kws,
        accreds,
    )
    result["sector_label"] = label
    result["confidence"] = conf

    return result


def cmd_run(args):
    """Run keyword extraction on input CSV."""
    init_db()

    df = pd.read_csv(args.input)
    if "domain" not in df.columns:
        print("ERROR: Input CSV must have a 'domain' column")
        sys.exit(1)

    # Filter out empty domains
    df = df[df["domain"].notna() & (df["domain"].str.strip() != "")]

    total = len(df)
    if args.limit:
        df = df.head(args.limit)
        total = len(df)

    # Resume: skip already processed
    processed = get_processed_domains()
    rows = [r.to_dict() for _, r in df.iterrows() if r["domain"].strip() not in processed]
    skipped = total - len(rows)

    if skipped:
        print(f"Resuming: {skipped} already processed, {len(rows)} remaining")

    if not rows:
        print("All domains already processed!")
        if args.output:
            cmd_export_to(args.output)
        return

    workers = args.workers or WORKERS
    if args.dry_run:
        workers = 1
        rows = rows[:10]

    print(f"Processing {len(rows)} domains with {workers} workers...")
    t0 = time.time()

    stats = {"success": 0, "failed": 0, "blocked": 0}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_one, row): row for row in rows}
        with tqdm(total=len(rows), desc="Extracting", unit="co") as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    save_result(result)
                    stats[result["scrape_status"]] = stats.get(result["scrape_status"], 0) + 1
                    if args.dry_run:
                        print(f"\n--- {result['domain']} ---")
                        for k, v in result.items():
                            if v:
                                print(f"  {k}: {str(v)[:120]}")
                except Exception as e:
                    stats["failed"] += 1
                    print(f"\nError: {e}")
                pbar.update(1)

    elapsed = time.time() - t0
    total_done = sum(stats.values())

    print(f"\n{'='*60}")
    print(f"DONE in {elapsed/60:.1f}m ({total_done/elapsed:.1f} domains/sec)")
    print(f"  Success: {stats['success']} | Failed: {stats['failed']} | Blocked: {stats['blocked']}")

    # Confidence distribution
    conn = sqlite3.connect(DB_PATH)
    for conf in ["HIGH", "MEDIUM", "LOW", "NONE"]:
        count = conn.execute(
            "SELECT COUNT(*) FROM keyword_extraction WHERE confidence=?", (conf,)
        ).fetchone()[0]
        print(f"  {conf}: {count}")

    # Top 20 sector labels
    print(f"\nTop 20 sector labels:")
    for label, cnt in conn.execute(
        "SELECT sector_label, COUNT(*) c FROM keyword_extraction GROUP BY sector_label ORDER BY c DESC LIMIT 20"
    ).fetchall():
        print(f"  {cnt:5d}  {label}")

    # Top accreditations
    print(f"\nTop accreditations detected:")
    all_acc = conn.execute(
        "SELECT accreditations FROM keyword_extraction WHERE accreditations != ''"
    ).fetchall()
    from collections import Counter
    acc_counts = Counter()
    for (accs,) in all_acc:
        for a in accs.split(", "):
            if a:
                acc_counts[a] += 1
    for acc, cnt in acc_counts.most_common(20):
        print(f"  {cnt:5d}  {acc}")

    print(f"\nTotal cost: FREE")
    conn.close()

    if args.output:
        cmd_export_to(args.output)


def cmd_export_to(output_path: str):
    """Export DB to CSV."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM keyword_extraction", conn)
    conn.close()
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows to {output_path}")


def cmd_export(args):
    cmd_export_to(args.output)


def cmd_status(args):
    """Show processing status."""
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM keyword_extraction").fetchone()[0]
    success = conn.execute("SELECT COUNT(*) FROM keyword_extraction WHERE scrape_status='success'").fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM keyword_extraction WHERE scrape_status='failed'").fetchone()[0]
    blocked = conn.execute("SELECT COUNT(*) FROM keyword_extraction WHERE scrape_status='blocked'").fetchone()[0]
    print(f"Total: {total} | Success: {success} | Failed: {failed} | Blocked: {blocked}")
    conn.close()


def cmd_retry(args):
    """Retry failed domains."""
    conn = sqlite3.connect(DB_PATH)
    failed = conn.execute(
        "SELECT domain, company_number, company_name FROM keyword_extraction WHERE scrape_status IN ('failed', 'blocked')"
    ).fetchall()
    conn.close()

    if not failed:
        print("No failed domains to retry!")
        return

    rows = [{"domain": d, "company_number": cn, "company_name": nm} for d, cn, nm in failed]
    # Delete old entries so they get reprocessed
    conn = sqlite3.connect(DB_PATH)
    for r in rows:
        conn.execute("DELETE FROM keyword_extraction WHERE domain=?", (r["domain"],))
    conn.commit()
    conn.close()

    print(f"Retrying {len(rows)} failed domains...")

    class FakeArgs:
        input = None
        output = args.output if hasattr(args, "output") else None
        workers = WORKERS
        limit = None
        dry_run = False

    # Process directly
    stats = {"success": 0, "failed": 0, "blocked": 0}
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_one, row): row for row in rows}
        with tqdm(total=len(rows), desc="Retrying") as pbar:
            for future in as_completed(futures):
                result = future.result()
                save_result(result)
                stats[result["scrape_status"]] = stats.get(result["scrape_status"], 0) + 1
                pbar.update(1)
    print(f"Done. Success: {stats['success']} | Failed: {stats['failed']} | Blocked: {stats['blocked']}")


def main():
    parser = argparse.ArgumentParser(description="UK company keyword extractor")
    sub = parser.add_subparsers(dest="command")

    p_run = sub.add_parser("run", help="Extract keywords from company websites")
    p_run.add_argument("--input", "-i", required=True, help="Input CSV with domain column")
    p_run.add_argument("--output", "-o", help="Output CSV path")
    p_run.add_argument("--workers", "-w", type=int, help=f"Concurrent workers (default {WORKERS})")
    p_run.add_argument("--limit", "-n", type=int, help="Process first N only")
    p_run.add_argument("--dry-run", action="store_true", help="Process 10, print to console")

    p_status = sub.add_parser("status", help="Show processing status")

    p_export = sub.add_parser("export", help="Export results to CSV")
    p_export.add_argument("--output", "-o", required=True, help="Output CSV path")

    p_retry = sub.add_parser("retry-failed", help="Retry failed domains")
    p_retry.add_argument("--output", "-o", help="Output CSV path")

    args = parser.parse_args()
    if args.command == "run":
        cmd_run(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "export":
        cmd_export(args)
    elif args.command == "retry-failed":
        cmd_retry(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
