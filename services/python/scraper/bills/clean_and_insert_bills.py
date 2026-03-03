#!/usr/bin/env python3
"""
Bills Data Cleaner and Database Inserter

This module is responsible for:
1. Loading scraped bills data from JSON
2. Cleaning and normalizing the data
3. Inserting into the database

Create your implementation here.
"""

import json
import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"


def load_bills_data(file_path: str = None):
    """Load bills data from JSON file."""
    if file_path is None:
        # Find the latest bills file
        output_dir = Path(__file__).parent.parent.parent / "data" / "output"
        bills_files = list(output_dir.glob("bills_*.json"))
        if bills_files:
            file_path = str(max(bills_files, key=lambda p: p.stat().st_mtime))
        else:
            # Fallback to scraper-local timestamped files
            local_files = list(DATA_DIR.glob("bills_*.json"))
            if local_files:
                file_path = str(max(local_files, key=lambda p: p.stat().st_mtime))
            else:
                file_path = str(DATA_DIR / "bills.json")

    log.info(f"Loading bills data from: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def clean_and_normalize(bills: list) -> list:
    """
    Clean and normalize bills data.

    TODO: Implement your cleaning logic here.
    Examples of what you might want to do:
    - Remove duplicates
    - Standardize date formats
    - Handle missing values
    - Validate data types
    """
    cleaned_bills = []
    seen = set()  # Track (registration_number, year) for dedup

    for bill in bills:
        # Create a cleaned copy
        cleaned = {
            "bill_id": bill.get("bill_id"),
            "type": bill.get("type"),  # HoR or NA
            "titleNp": bill.get("titleNp", ""),
            "titleEn": bill.get("titleEn", ""),
            "registration_number": bill.get("registration_number"),
            "year": bill.get("year"),
            "sambat": bill.get("sambat"),
            "presenter": bill.get("presenter"),
            "ministry": bill.get("ministry"),
            "presenter_en": bill.get("presenterEn") or bill.get("presenter_en"),
            "ministry_en": bill.get("ministryEn") or bill.get("ministry_en"),
            "session": bill.get("session"),
            "government_type": bill.get("government_type"),
            "bill_type": bill.get("bill_type"),
            "category": bill.get("category"),
            "government_type_en": bill.get("government_type_en"),
            "bill_type_en": bill.get("bill_type_en"),
            "category_en": bill.get("category_en"),
            "current_status": bill.get("current_status"),
            "current_status_date": bill.get("current_status_date"),
            "status_timeline": bill.get("status_timeline"),
            "resource_link": bill.get("resource_link"),
        }

        # Skip bills without essential identifiers
        if not cleaned.get("bill_id") or not cleaned.get("registration_number"):
            log.warning(f"Skipping bill with missing bill_id or registration_number")
            continue

        # Deduplicate by (registration_number, sambat/year)
        dedup_key = f"{cleaned.get('registration_number')}_{cleaned.get('sambat') or cleaned.get('year', '')}"
        if dedup_key in seen:
            log.debug(f"Skipping duplicate bill: {dedup_key}")
            continue
        seen.add(dedup_key)

        cleaned_bills.append(cleaned)

    log.info(f"Cleaned {len(cleaned_bills)} bills (from {len(bills)} raw, {len(bills) - len(cleaned_bills)} removed)")
    return cleaned_bills


def insert_to_database(bills: list):
    """
    Previously inserted cleaned bills directly into Postgres.
    This responsibility has been moved to the Bun/TypeScript importer.
    """
    log.info(
        "Skipping direct DB insert in Python. "
        "Use the Bun script (e.g. `bun db:import-bills`) to load bills into the database."
    )
    return {"success": True, "inserted_count": 0}


def main():
    """Main entry point."""
    try:
        log.info("="*60)
        log.info("Bills Cleaner & Inserter")
        log.info("="*60)

        # Load data
        bills = load_bills_data()
        log.info(f"Loaded {len(bills)} bills")

        # Clean and normalize
        cleaned_bills = clean_and_normalize(bills)

        # Save cleaned data (optional)
        output_dir = Path(__file__).parent.parent.parent / "data" / "output"
        output_file = output_dir / "bills_cleaned.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_bills, f, ensure_ascii=False, indent=2)
        log.info(f"Saved cleaned data to: {output_file}")

        # Insert into database
        result = insert_to_database(cleaned_bills)

        log.info("="*60)
        log.info("Completed!")
        log.info("="*60)

        return result

    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    main()
