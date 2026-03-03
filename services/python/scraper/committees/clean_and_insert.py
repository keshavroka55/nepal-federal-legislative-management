#!/usr/bin/env python3
"""
Committees Data Cleaner and Database Inserter

This module is responsible for:
1. Loading scraped committees data from JSON
2. Cleaning and normalizing data
3. Preparing for database insertion

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


def load_committees_data(file_path: str = None):
    """Load committees data from JSON file."""
    if file_path is None:
        # Find the latest committees file
        output_dir = Path(__file__).parent.parent.parent / "data" / "output"
        committees_files = list(output_dir.glob("committees_*.json"))
        if committees_files:
            file_path = str(max(committees_files, key=lambda p: p.stat().st_mtime))
        else:
            # Try default location in scraper directory
            files = list(DATA_DIR.glob("committees_*.json"))
            if files:
                file_path = str(max(files, key=lambda p: p.stat().st_mtime))
            else:
                file_path = str(DATA_DIR / "committees.json")

    log.info(f"Loading committees data from: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def clean_and_normalize(committees: list) -> list:
    """
    Clean and normalize committees data.

    Examples of what we do:
    - Remove duplicates by slug
    - Handle missing values
    - Ensure all required fields are present
    - Normalize house enum values
    """
    cleaned_committees = []
    seen_slugs = set()  # Track (house, slug) for dedup

    for committee in committees:
        # Create a cleaned copy
        cleaned = {
            "house": committee.get("house"),
            "houseEnum": committee.get("houseEnum"),  # pratinidhi_sabha or rastriya_sabha
            "slug": committee.get("slug"),
            "nameNp": committee.get("nameNp", ""),
            "nameEn": committee.get("nameEn", ""),
            "introductionNp": committee.get("introductionNp", ""),
            "introductionEn": committee.get("introductionEn", ""),
            "chairperson": committee.get("chairperson", ""),
            "chairpersonNp": committee.get("chairpersonNp", ""),
            "chairpersonEn": committee.get("chairpersonEn", ""),
            "secretaryNp": committee.get("secretaryNp", ""),
            "secretaryEn": committee.get("secretaryEn", ""),
            "menuLinksNp": committee.get("menuLinksNp", {}) or {},
            "menuLinksEn": committee.get("menuLinksEn", {}) or {},
            "membersPageUrlNp": committee.get("membersPageUrlNp"),
            "membersPageUrlEn": committee.get("membersPageUrlEn"),
            "parliamentUrlNp": committee.get("parliamentUrlNp"),
            "parliamentUrlEn": committee.get("parliamentUrlEn"),
        }

        # Skip committees without essential identifiers
        if not cleaned.get("slug") or not cleaned.get("house"):
            log.warning(f"Skipping committee with missing slug or house")
            continue

        # Deduplicate by (house, slug)
        dedup_key = f"{cleaned.get('house')}_{cleaned.get('slug')}"
        if dedup_key in seen_slugs:
            log.debug(f"Skipping duplicate committee: {dedup_key}")
            continue
        seen_slugs.add(dedup_key)

        cleaned_committees.append(cleaned)

    log.info(f"Cleaned {len(cleaned_committees)} committees (from {len(committees)} raw, {len(committees) - len(cleaned_committees)} removed)")
    return cleaned_committees


def save_cleaned_data(committees: list):
    """Save cleaned committees data to JSON file."""
    output_dir = Path(__file__).parent.parent.parent / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "committees_cleaned.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(committees, f, ensure_ascii=False, indent=2)
    log.info(f"Saved cleaned data to: {output_file}")

    return output_file


def insert_to_database(committees: list):
    """
    Database insertion is delegated to Bun/TypeScript importer.
    """
    log.info(
        "Skipping direct DB insert in Python. "
        "Use Bun script to load committees into database."
    )
    return {"success": True, "inserted_count": 0}


def main():
    """Main entry point."""
    try:
        log.info("="*60)
        log.info("Committees Cleaner & Inserter")
        log.info("="*60)

        # Load data
        committees = load_committees_data()
        log.info(f"Loaded {len(committees)} committees")

        # Clean and normalize
        cleaned_committees = clean_and_normalize(committees)

        # Save cleaned data
        output_file = save_cleaned_data(cleaned_committees)

        # Insert into database (delegated to Bun script)
        result = insert_to_database(cleaned_committees)

        log.info("="*60)
        log.info("Completed!")
        log.info("="*60)

        return result

    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    main()
