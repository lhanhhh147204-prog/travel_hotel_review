# scrapers/traveloka.py
from __future__ import annotations

import asyncio
import random
import re

from playwright.async_api import BrowserContext, Page

from core.helpers import (
    _first_text, _text,
    clean, extract_city_slug,
    open_page, human_scroll,
    parse_price,
    PAGE_DELAY_MIN, PAGE_DELAY_MAX,
    MAX_PAGES_PER_HOTEL,
    log,
)


async def _traveloka_intercept_api(page: Page) -> list[dict]:
    """Intercept Traveloka GraphQL API để lấy review data."""
    reviews_api: list[dict] = []

    async def handle_response(response):
        try:
            if (
                "hotel/v2/review"    in response.url
                or "getHotelReviews" in response.url
            ):
                data  = await response.json()
                items = (
                    data.get("data", {})
                    .get("hotelReviews", {})
                    .get("reviews", [])
                    or data.get("reviews", [])
                    or []
                )
                for r in items:
                    text = (
                        r.get("reviewText", "")
                        or r.get("content", "")
                        or ""
                    )
                    if text:
                        reviews_api.append({
                            "reviewer": (
                                r.get("reviewerName")
                                or r.get("userName")
                                or "Ẩn danh"
                            )[:200],
                            "score":    float(
                                r.get("rating", 0)
                                or r.get("score",  0)
                            ),
                            "text":     text[:3000],
                            "date":     r.get("reviewDate"),
                            "country":  r.get("reviewerCountry"),
                            "lang":     r.get("languageCode", "vi"),
                            "platform": "traveloka.com",
                        })
        except Exception:
            pass

    page.on("response", handle_response)
    return reviews_api


async def _traveloka_parse_rooms(page: Page) -> list[dict]:
    rooms = []
    rows  = page.locator(
        "[data-testid='room-card'], "
        ".room-type-container, "
        "[class*='RoomCard'], "
        "[class*='room-list-item']"
    )
    for i in range(await rows.count()):
        row = rows.nth(i)
        try:
            name_el  = row.locator(
                "[data-testid='room-name'], "
                "[class*='RoomName'], "
                "h3, h4"
            ).first
            price_el = row.locator(
                "[data-testid='room-price'], "
                "[class*='Price'], "
                "[class*='price-value']"
            ).first
            name  = await _text(name_el)  if await name_el.count()  > 0 else None
            price = parse_price(
                await _text(price_el) if await price_el.count() > 0 else ""
            )
            avail     = True
            sold_el   = row.locator(
                "[class*='SoldOut'], "
                "[class*='sold-out'], "
                ".het-phong"
            ).first
            if await sold_el.count() > 0:
                avail = False

            if name and price:
                rooms.append({
                    "name":      name[:200],
                    "price":     price,
                    "available": avail,
                })
        except Exception:
            continue
    return rooms


async def _traveloka_parse_reviews_html(page: Page) -> list[dict]:
    """Fallback: parse HTML nếu API không intercept được."""
    batch: list[dict] = []
    cards = page.locator(
        "[data-testid='review-item'], "
        "[class*='ReviewCard'], "
        "[class*='review-item']"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    "[data-testid='review-text'], "
                    "[class*='ReviewText'], "
                    "p.review-content"
                ).first
            )
            if not text:
                continue

            score_raw = await _text(
                c.locator(
                    "[data-testid='review-score'], "
                    "[class*='RatingScore'], "
                    "span.score"
                ).first
            )
            try:
                score = float(
                    re.sub(r"[^\d.,]", "", score_raw)
                    .replace(",", ".")
                    or "0"
                )
            except ValueError:
                score = 0.0

            batch.append({
                "reviewer": clean(await _text(
                    c.locator(
                        "[data-testid='reviewer-name'], "
                        "[class*='ReviewerName'], "
                        "span.user-name"
                    ).first
                )) or "Ẩn danh",
                "score":    score,
                "text":     text[:3000],
                "date":     clean(await _text(
                    c.locator(
                        "[data-testid='review-date'], "
                        "[class*='ReviewDate'], "
                        "span.date"
                    ).first
                )),
                "lang":     "vi",
                "platform": "traveloka.com",
            })
        except Exception:
            continue
    return batch


async def scrape_traveloka(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Traveloka.com."""
    hotel: dict = {
        "url":      url,
        "platform": "traveloka.com",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Setup API interceptor ─────────────────────────────────
    api_reviews = await _traveloka_intercept_api(page)
    # ── Tên (tiếp) ───────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "[data-testid='hotel-name']",
        "h1[class*='HotelName']",
        "h1[class*='hotel-name']",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "[data-testid='hotel-address']",
        "[class*='HotelAddress']",
        ".hotel-address",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_els       = page.locator(
        "[data-testid='star-rating'] svg, "
        "[class*='StarIcon'].active, "
        "span.star-filled"
    )
    hotel["stars"] = min(await star_els.count(), 5)

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _traveloka_parse_rooms(page)

    # ── Click tab review ──────────────────────────────────────
    for tab_sel in [
        "[data-testid='review-tab']",
        "button:has-text('Đánh giá')",
        "a:has-text('Reviews')",
        "[class*='ReviewTab']",
        "li:has-text('Review') a",
    ]:
        tab = page.locator(tab_sel).first
        if await tab.count() > 0:
            try:
                await tab.click()
                await asyncio.sleep(random.uniform(1.5, 2.5))
                break
            except Exception:
                pass

    # ── Reviews ───────────────────────────────────────────────
    all_reviews: list[dict] = []

    if api_reviews:
        # ── Ưu tiên dữ liệu từ API interceptor ───────────────
        all_reviews = api_reviews
        log.info(
            f"    Traveloka API: {len(all_reviews)} reviews"
        )
    else:
        # ── Fallback: parse HTML + click next ─────────────────
        for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
            batch = await _traveloka_parse_reviews_html(page)
            if not batch:
                log.info(
                    f"    Traveloka: hết review ở trang {page_num}"
                )
                break

            all_reviews.extend(batch)
            log.info(
                f"    Traveloka trang {page_num}"
                f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
            )

            # ── Tìm nút next ──────────────────────────────────
            next_found = False
            for next_sel in [
                "[data-testid='next-page']",
                "button:has-text('Tiếp theo')",
                "button[aria-label='Next']",
                ".pagination-next",
            ]:
                btn = page.locator(next_sel).first
                if await btn.count() > 0:
                    try:
                        await btn.click()
                        await asyncio.sleep(
                            random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                        )
                        await human_scroll(page)
                        next_found = True
                        break
                    except Exception:
                        pass

            if not next_found:
                break

    return hotel, rooms, all_reviews