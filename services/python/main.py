#!/usr/bin/env python3
"""
Nepal Federal Legislative Scraper - Main Controller

Controls scraping for both Bills and Committees from Nepal Parliament.
Runs in automated mode (no prompts) suitable for cron jobs.

Usage:
    python main.py                    # Auto-run: scrape bills, committees, clean both
"""

import asyncio
import json
import logging
import sys
import inspect
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "data" / "output"
SCRAPER_DIR = BASE_DIR / "scraper"

# Create output directories
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================================
# SCRAPER IMPORTS
# =====================================================================

def import_bills_scraper():
    """Import bills scraper module."""
    sys.path.insert(0, str(SCRAPER_DIR / "bills"))
    try:
        import scrape_bills
        return scrape_bills
    except ImportError as e:
        log.error(f"Failed to import bills scraper: {e}")
        return None

def import_bills_cleaner():
    """Import bills cleaner/normalizer module."""
    sys.path.insert(0, str(SCRAPER_DIR / "bills"))
    try:
        import clean_and_insert_bills
        return clean_and_insert_bills
    except ImportError:
        return None

def import_committees_scraper():
    """Import committees scraper module."""
    sys.path.insert(0, str(SCRAPER_DIR / "committees"))
    try:
        import scrape_committees
        return scrape_committees
    except ImportError as e:
        log.error(f"Failed to import committees scraper: {e}")
        return None

def import_committees_cleaner():
    """Import committees cleaner module."""
    sys.path.insert(0, str(SCRAPER_DIR / "committees"))
    try:
        import clean_and_insert
        return clean_and_insert
    except ImportError:
        return None

# =====================================================================
# MAIN AUTOMATED WORKFLOW
# =====================================================================

async def run_all():
    """
    Run all scrapers and cleaners automatically.
    This is designed for cron jobs or automated runs.
    """
    log.info("=" * 60)
    log.info("AUTO-RUN MODE: Scraping bills, then committees, then cleaning both")
    log.info("=" * 60)

    results = {}

    # Scrape bills first
    log.info("\n[1/3] Scraping bills...")
    scrape_bills = import_bills_scraper()
    if scrape_bills:
        try:
            bills_result = await scrape_bills.scrape_all()
            if isinstance(bills_result, list):
                hor_count = sum(1 for b in bills_result if b.get("type") == "HoR")
                na_count = sum(1 for b in bills_result if b.get("type") == "NA")
                results["bills"] = {
                    "success": True,
                    "total_bills": len(bills_result),
                    "hor_count": hor_count,
                    "na_count": na_count,
                }
            else:
                results["bills"] = bills_result
        except Exception as e:
            log.error(f"Bills scraping failed: {e}", exc_info=True)
            results["bills"] = {"success": False, "error": str(e)}
    else:
        log.warning("Bills scraper module not available, skipping...")
        results["bills"] = None

    # Then scrape committees
    log.info("\n[2/3] Scraping committees...")
    scrape_committees = import_committees_scraper()
    if scrape_committees:
        try:
            committees_result = await scrape_committees.scrape_all_committees()
            if isinstance(committees_result, list):
                hor_count = sum(1 for c in committees_result if c.get("house") == "HoR")
                na_count = sum(1 for c in committees_result if c.get("house") == "NA")
                results["committees"] = {
                    "success": True,
                    "total_committees": len(committees_result),
                    "hor_count": hor_count,
                    "na_count": na_count,
                }
            else:
                results["committees"] = committees_result
        except Exception as e:
            log.error(f"Committee scraping failed: {e}", exc_info=True)
            results["committees"] = {"success": False, "error": str(e)}
    else:
        log.warning("Committees scraper module not available, skipping...")
        results["committees"] = None

    # Clean both
    log.info("\n[3/3] Cleaning bills and committees...")
    bills_cleaner = import_bills_cleaner()
    if bills_cleaner:
        try:
            bills_clean_result = bills_cleaner.main()
            if inspect.isawaitable(bills_clean_result):
                bills_clean_result = await bills_clean_result
            results["bills_clean"] = bills_clean_result
        except Exception as e:
            log.error(f"Bills cleaner failed: {e}", exc_info=True)
            results["bills_clean"] = {"success": False, "error": str(e)}
    else:
        log.warning("Bills cleaner module not available, skipping...")
        results["bills_clean"] = None

    committees_cleaner = import_committees_cleaner()
    if committees_cleaner:
        try:
            committees_clean_result = committees_cleaner.main()
            if inspect.isawaitable(committees_clean_result):
                committees_clean_result = await committees_clean_result
            results["committees_clean"] = committees_clean_result
        except Exception as e:
            log.error(f"Committees cleaner failed: {e}", exc_info=True)
            results["committees_clean"] = {"success": False, "error": str(e)}
    else:
        log.warning("Committees cleaner module not available, skipping...")
        results["committees_clean"] = None

    # Print and save report
    print_report(results)
    return results


# =====================================================================
# REPORTING
# =====================================================================

def save_report(results: dict):
    """Save JSON report for each run."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_file = OUTPUT_DIR / f"run_report_{timestamp}.json"

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"Saved report: {report_file}")

def print_report(results: dict):
    """Print a formatted report of scraping results."""
    log.info("\n" + "=" * 60)
    log.info("SCRAPING REPORT")
    log.info("=" * 60)

    for scraper_name, result in results.items():
        if result is None:
            continue

        if isinstance(result, list):
            # Backward compatibility for any scraper returning raw list data.
            if scraper_name == "bills":
                result = {
                    "success": True,
                    "total_bills": len(result),
                    "hor_count": sum(1 for b in result if b.get("type") == "HoR"),
                    "na_count": sum(1 for b in result if b.get("type") == "NA"),
                }
            elif scraper_name == "committees":
                result = {
                    "success": True,
                    "total_committees": len(result),
                    "hor_count": sum(1 for c in result if c.get("house") == "HoR"),
                    "na_count": sum(1 for c in result if c.get("house") == "NA"),
                }
            else:
                result = {"success": True}

        log.info(f"\n{scraper_name.upper()}:")
        if isinstance(result, dict) and result.get("success"):
            log.info(f"  Status: Success")
            if "duration_seconds" in result:
                log.info(f"  Duration: {result['duration_seconds']:.2f}s")
            if "total_bills" in result:
                log.info(f"  Total bills: {result['total_bills']}")
                hor_count = result.get('hor_count', 0)
                na_count = result.get('na_count', 0)
                if hor_count > 0:
                    log.info(f"    HoR: {hor_count}")
                if na_count > 0:
                    log.info(f"    NA:  {na_count}")
            if "total_committees" in result:
                log.info(f"  Total committees: {result['total_committees']}")
                hor_count = result.get('hor_count', 0)
                na_count = result.get('na_count', 0)
                if hor_count > 0:
                    log.info(f"    HoR: {hor_count}")
                if na_count > 0:
                    log.info(f"    NA:  {na_count}")
            if "output" in result:
                log.info(f"  Output: {result['output']}")
        else:
            log.info(f"  Status: Failed")
            error = result.get('error', 'Unknown error') if isinstance(result, dict) else "Unknown error"
            log.info(f"  Error: {error}")

    log.info("\n" + "=" * 60 + "\n")

    # Save report to file if not --no-report
    save_report(results)

    log.info("=" * 60 + "\n")


# =====================================================================
# MAIN ENTRY POINT
# =====================================================================

def main():
    """Main entry point - runs in automated mode by default."""
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
