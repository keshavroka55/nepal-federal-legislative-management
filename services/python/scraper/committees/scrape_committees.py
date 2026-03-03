#!/usr/bin/env python3
"""
Nepal Parliament Committees Scraper

Scrapes committee detail pages for:
- HoR (House of Representatives)
- NA  (National Assembly)

Each committee is scraped in both Nepali and English.
"""

import argparse
import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR.parent.parent / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


PARLIAMENT_URLS = {
    "HoR": "https://hr.parliament.gov.np",
    "NA": "https://na.parliament.gov.np",
}

HOUSE_MAPPING = {
    "HoR": "pratinidhi_sabha",
    "NA": "rastriya_sabha",
}

COMMITTEES = {
    "HoR": [
        "Finance-Committee",
        "International-Relations-and-Tourism-Committee",
        "Industry-Commerce-Labour-and-Consumer-Welfare-Committee",
        "Law-Justice-and-Human-Rights-Committee",
        "Agriculture-Cooperative-And-Natural-Resources-Committee",
        "Women-And-Social-Affairs-Committee",
        "State-Affairs-and-Good-Governance-Committee",
        "Infrastructure-Development-Committee",
        "Education-Health-and-Information-Technology-Committee",
        "Public-Account-Committee",
        "Parliamentary-Hearing-Committee",
        "Monitoring-and-Evaluation-of-the-Implementation-of-the-Directive-Principles-Policies-and-Obligations-of-the-state-Committee-2080",
    ],
    "NA": [
        "Committee-on-Development-Economic-Affairs-and-Good-Governance",
        "Committee-for-Legislation-Management",
        "Public-Policy-and-Delegated-Legislation-Committee",
        "Federalism-Enablement-and-National-Concerns-Committee",
        "Parliamentary-Hearing-Committee",
        "Monitoring-and-Evaluation-of-the-Implementation-of-the-Directive-Principles-Policies-and-Obligations-of-the-state-Committee-2080",
        "Sanghiyata-Karyanwoyen-Addheyen-tatha-Anugaman-Samsadiya-Bisesh-Samiti",
    ],
}


def clean_text(text: str) -> str:
    """Normalize raw scraped text."""
    if not text:
        return ""

    text = text.replace("\u200b", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n|\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_inline_text(text: str) -> str:
    """Normalize inline labels/names."""
    return re.sub(r"\s+", " ", text or "").strip(" ,\n\t")


def to_absolute_url(base_url: str, maybe_url: str) -> str:
    """Build an absolute URL from relative or absolute input."""
    return urljoin(base_url + "/", (maybe_url or "").strip())


def extract_menu_links(
    soup: BeautifulSoup,
    base_url: str,
) -> Dict[str, str]:
    """Extract committee menu link label -> URL mapping."""
    menu_links: Dict[str, str] = {}
    for anchor in soup.select("div.committee-menu a[href]"):
        label_el = anchor.select_one("div.work-description") or anchor
        label = clean_inline_text(label_el.get_text(" ", strip=True))
        href = to_absolute_url(base_url, anchor.get("href", ""))
        if label and href:
            menu_links[label] = href
    return menu_links


def extract_members_page_url(
    soup: BeautifulSoup,
    base_url: str,
) -> Optional[str]:
    """Extract committee members page URL from sidebar button."""
    for anchor in soup.select("div.speaker-profile a[href], a.btn[href]"):
        text = clean_inline_text(anchor.get_text(" ", strip=True)).lower()
        if (
            "समिति सदस्य" in text
            or "committee member" in text
            or text == "members"
            or text.endswith("members")
        ):
            return to_absolute_url(base_url, anchor.get("href", ""))
    return None


def extract_people_roles(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Extract chairperson/secretary names from sidebar media blocks.

    The same block usually contains:
    - linked person name
    - role label
    """
    role_pairs: List[Dict[str, str]] = []

    for media in soup.select("div.speaker-profile div.media"):
        name = ""
        role = ""

        name_el = media.select_one("a strong")
        if name_el:
            name = clean_inline_text(name_el.get_text(" ", strip=True))

        role_candidates = [
            clean_inline_text(el.get_text(" ", strip=True))
            for el in media.select("span strong")
        ]
        role_candidates = [r for r in role_candidates if r and r != name]
        if role_candidates:
            role = role_candidates[-1]

        if name and role:
            role_pairs.append({"name": name, "role": role})

    chairperson = ""
    secretary = ""

    chair_keywords = ("chairperson", "chair", "सभापति", "अध्यक्ष")
    secretary_keywords = ("secretary", "सचिव")

    for pair in role_pairs:
        role_lower = pair["role"].lower()
        if not chairperson and any(k in role_lower for k in chair_keywords):
            chairperson = pair["name"]
        if not secretary and any(k in role_lower for k in secretary_keywords):
            secretary = pair["name"]

    return {
        "chairperson": chairperson,
        "secretary": secretary,
    }


class CommitteesHTTPClient:
    """HTTP client for committee scraping."""

    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            timeout=30.0,
            verify=False,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def get_html(self, url: str) -> Optional[str]:
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as exc:
            log.error("HTTP error for %s: %s", url, exc)
            return None
        except Exception as exc:  # broad catch for resilience
            log.error("Request failed for %s: %s", url, exc)
            return None


class CommitteeDetailScraper:
    """Extract data from a single committee detail page."""

    def __init__(self, client: CommitteesHTTPClient) -> None:
        self.client = client

    async def scrape_committee_detail(
        self,
        house: str,
        slug: str,
        lang: str,
    ) -> Dict[str, Any]:
        base_url = PARLIAMENT_URLS[house]
        detail_url = f"{base_url}/{lang}/committees/{slug}"

        log.info("Fetching %s", detail_url)
        html = await self.client.get_html(detail_url)
        if not html:
            return {}

        soup = BeautifulSoup(html, "lxml")

        title_el = soup.select_one("section.single-post h1") or soup.find("h1")
        intro_el = soup.select_one("div.committee-description")

        intro_text = ""
        if intro_el:
            intro_text = clean_text(intro_el.get_text("\n", strip=True))

        people = extract_people_roles(soup)
        menu_links = extract_menu_links(soup, base_url)
        members_page_url = extract_members_page_url(soup, base_url)

        return {
            "house": house,
            "slug": slug,
            "language": lang,
            "name": clean_inline_text(title_el.get_text(" ", strip=True))
            if title_el
            else "",
            "introduction": intro_text,
            "chairperson": people.get("chairperson", ""),
            "secretary": people.get("secretary", ""),
            "menuLinks": menu_links,
            "membersPageUrl": members_page_url,
            "sourceUrl": detail_url,
        }

    async def scrape_committee_both_languages(
        self,
        house: str,
        slug: str,
    ) -> Dict[str, Any]:
        base_url = PARLIAMENT_URLS[house]

        np_data = await self.scrape_committee_detail(house, slug, "np")
        await asyncio.sleep(0.2)
        en_data = await self.scrape_committee_detail(house, slug, "en")

        if not np_data and not en_data:
            return {}

        members_page_np = (
            np_data.get("membersPageUrl")
            or f"{base_url}/np/committees/{slug}/members"
        )
        members_page_en = (
            en_data.get("membersPageUrl")
            or f"{base_url}/en/committees/{slug}/members"
        )

        return {
            "house": house,
            "houseEnum": HOUSE_MAPPING[house],
            "slug": slug,
            "nameNp": np_data.get("name", ""),
            "nameEn": en_data.get("name", ""),
            "introductionNp": np_data.get("introduction", ""),
            "introductionEn": en_data.get("introduction", ""),
            "chairperson": np_data.get("chairperson")
            or en_data.get("chairperson")
            or "",
            "chairpersonNp": np_data.get("chairperson", ""),
            "chairpersonEn": en_data.get("chairperson", ""),
            "secretaryNp": np_data.get("secretary", ""),
            "secretaryEn": en_data.get("secretary", ""),
            "menuLinksNp": np_data.get("menuLinks", {}),
            "menuLinksEn": en_data.get("menuLinks", {}),
            "membersPageUrlNp": members_page_np,
            "membersPageUrlEn": members_page_en,
            "parliamentUrlNp": f"{base_url}/np/committees/{slug}",
            "parliamentUrlEn": f"{base_url}/en/committees/{slug}",
            "scrapedAt": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }


class CommitteesScraper:
    """Main committees scraper orchestrator."""

    def __init__(self) -> None:
        self.client = CommitteesHTTPClient()
        self.detail_scraper = CommitteeDetailScraper(self.client)

    async def close(self) -> None:
        await self.client.close()

    async def __aenter__(self) -> "CommitteesScraper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def scrape_house_committees(self, house: str) -> List[Dict[str, Any]]:
        slugs = COMMITTEES.get(house, [])
        house_committees: List[Dict[str, Any]] = []

        log.info("%s", "=" * 60)
        log.info("Scraping %s committees (%d)", house, len(slugs))
        log.info("%s", "=" * 60)

        for idx, slug in enumerate(slugs, start=1):
            log.info("[%d/%d] %s %s", idx, len(slugs), house, slug)
            try:
                data = await self.detail_scraper.scrape_committee_both_languages(
                    house,
                    slug,
                )
                if data:
                    house_committees.append(data)
            except Exception as exc:  # broad catch to continue scraping
                log.error("Failed scraping %s/%s: %s", house, slug, exc)

            await asyncio.sleep(0.35)

        return house_committees

    async def scrape_all(self) -> List[Dict[str, Any]]:
        all_committees: List[Dict[str, Any]] = []

        try:
            all_committees.extend(await self.scrape_house_committees("HoR"))
        except Exception as exc:
            log.error("Error scraping HoR: %s", exc)

        try:
            all_committees.extend(await self.scrape_house_committees("NA"))
        except Exception as exc:
            log.error("Error scraping NA: %s", exc)

        return all_committees


def get_output_filename() -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return str(OUTPUT_DIR / f"committees_{timestamp}.json")


def save_to_json(committees: List[Dict[str, Any]], output_file: str) -> None:
    with open(output_file, "w", encoding="utf-8") as fp:
        json.dump(committees, fp, ensure_ascii=False, indent=2)
    log.info("Saved %d committees to %s", len(committees), output_file)


async def scrape_all_committees(output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Async function used by services/python/main.py.
    Returns summary dict + raw data.
    """
    started_at = datetime.utcnow()

    try:
        async with CommitteesScraper() as scraper:
            all_committees = await scraper.scrape_all()

        resolved_output = output_file or get_output_filename()
        save_to_json(all_committees, resolved_output)

        hor_count = sum(1 for c in all_committees if c.get("house") == "HoR")
        na_count = sum(1 for c in all_committees if c.get("house") == "NA")

        return {
            "success": True,
            "total_committees": len(all_committees),
            "hor_count": hor_count,
            "na_count": na_count,
            "output": resolved_output,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            "data": all_committees,
        }
    except Exception as exc:
        log.error("Committee scraping failed: %s", exc, exc_info=True)
        return {
            "success": False,
            "error": str(exc),
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            "data": [],
        }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Nepal Parliament Committees Scraper")
    parser.add_argument(
        "--type",
        choices=["HoR", "NA", "all"],
        default="all",
        help="House to scrape (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output path for scraped JSON",
    )
    args = parser.parse_args()

    try:
        async with CommitteesScraper() as scraper:
            all_committees: List[Dict[str, Any]] = []

            if args.type in ("all", "HoR"):
                all_committees.extend(await scraper.scrape_house_committees("HoR"))
            if args.type in ("all", "NA"):
                all_committees.extend(await scraper.scrape_house_committees("NA"))

        output_file = args.output or get_output_filename()
        save_to_json(all_committees, output_file)

        hor_count = sum(1 for c in all_committees if c.get("house") == "HoR")
        na_count = sum(1 for c in all_committees if c.get("house") == "NA")

        log.info("%s", "=" * 60)
        log.info("SUMMARY")
        log.info("Total committees: %d", len(all_committees))
        log.info("  HoR: %d", hor_count)
        log.info("  NA:  %d", na_count)
        log.info("Output: %s", output_file)
        log.info("%s", "=" * 60)
    except Exception as exc:
        log.error("Unhandled error: %s", exc, exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
