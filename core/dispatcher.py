# core/dispatcher.py
# core/dispatcher.py
from __future__ import annotations

import logging
from urllib.parse import urlparse
from playwright.async_api import Page, BrowserContext

from scrapers.booking      import scrape_booking
from scrapers.agoda        import scrape_agoda
from scrapers.tripadvisor  import scrape_tripadvisor
from scrapers.google_maps  import scrape_google_maps
from scrapers.ivivu        import scrape_ivivu
from scrapers.mytour       import scrape_mytour
from scrapers.traveloka    import scrape_traveloka
from scrapers.vntrip       import scrape_vntrip
from scrapers.airbnb       import scrape_airbnb

log = logging.getLogger(__name__)

# ── Mapping domain → scraper function ────────────────────────
_SCRAPERS = {
    "booking.com":     scrape_booking,
    "agoda.com":       scrape_agoda,
    "tripadvisor.com": scrape_tripadvisor,
    "google.com":      scrape_google_maps,
    "ivivu.com":       scrape_ivivu,
    "mytour.vn":       scrape_mytour,
    "traveloka.com":   scrape_traveloka,
    "vntrip.vn":       scrape_vntrip,
    "airbnb.com":      scrape_airbnb,
}

_EMPTY_HOTEL = {
    "url":      "",
    "platform": "unknown",
    "name":     "",
    "city":     "Vietnam",
    "stars":    0,
    "address":  None,
    "type":     "Khách Sạn",
}


async def extract(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Điều phối đến scraper phù hợp theo domain."""
    domain = urlparse(url).netloc.lower()

    for key, scraper in _SCRAPERS.items():
        if key in domain:
            try:
                hotel, rooms, reviews = await scraper(page, url, ctx)

                # ── Validate & normalize kết quả ──────────────
                hotel.setdefault("name",     "Unknown")
                hotel.setdefault("city",     "Vietnam")
                hotel.setdefault("stars",    0)
                hotel.setdefault("address",  None)
                hotel.setdefault("type",     "Khách Sạn")
                hotel.setdefault("platform", key)
                hotel.setdefault("url",      url)

                # Lọc rooms không hợp lệ
                rooms = [
                    r for r in rooms
                    if r.get("price") and r.get("name")
                ]

                # Lọc reviews không hợp lệ
                reviews = [
                    r for r in reviews
                    if r.get("text") and len(r["text"].strip()) >= 10
                ]

                log.debug(
                    f"Extract OK: {hotel['name'][:30]} | "
                    f"rooms={len(rooms)} | "
                    f"reviews={len(reviews)}"
                )
                return hotel, rooms, reviews

            except Exception as e:
                log.error(
                    f"Scraper [{key}] lỗi tại {url[:60]}: {e}",
                    exc_info=True,
                )
                raise

    # ── Domain chưa hỗ trợ ───────────────────────────────────
    log.warning(f"⚠️  Domain chưa hỗ trợ: {domain}")
    result          = dict(_EMPTY_HOTEL)
    result["url"]      = url
    result["platform"] = domain
    return result, [], []