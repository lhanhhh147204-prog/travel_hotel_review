# scrapers/agoda.py
from __future__ import annotations

import asyncio
import json
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


async def _agoda_parse_rooms(page: Page) -> list[dict]:
    rooms = []
    rows  = page.locator(
        "[data-selenium='room-row'], "
        "div.RoomGrid-module__roomRow, "
        "li.room-type-item"
    )
    for i in range(await rows.count()):
        row = rows.nth(i)
        try:
            name_el  = row.locator(
                "[data-selenium='room-name'], "
                "span.room-name, h3"
            ).first
            price_el = row.locator(
                "[data-selenium='display-price'], "
                "span.price-display, "
                "strong.price"
            ).first
            name  = await _text(name_el)  if await name_el.count()  > 0 else None
            price = parse_price(
                await _text(price_el) if await price_el.count() > 0 else ""
            )
            if name and price:
                rooms.append({
                    "name":      name[:200],
                    "price":     price,
                    "available": True,
                })
        except Exception:
            continue
    return rooms


async def _agoda_api_reviews(
        rev_page: Page,
        hotel_id: str,
) -> list[dict]:
    """Cào review qua Agoda internal API."""
    all_reviews: list[dict] = []

    for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
        api_url = (
            f"https://www.agoda.com/api/cronos/property"
            f"/review/hotelReviews"
            f"?hotelId={hotel_id}"
            f"&pageNo={page_num}"
            f"&pageSize=10"
            f"&languageId=1"
            f"&sortBy=recency"
        )
        try:
            await rev_page.goto(
                api_url,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            content = await rev_page.content()
            json_m  = re.search(r"\{.*\}", content, re.DOTALL)
            if not json_m:
                break

            data    = json.loads(json_m.group())
            reviews = (
                data.get("reviewList", [])
                or data.get("data", {}).get("reviews", [])
                or []
            )
            if not reviews:
                break

            for r in reviews:
                text = (
                    r.get("reviewText", "")
                    or r.get("comment", "")
                    or (
                        f"{r.get('positiveComment', '')} "
                        f"{r.get('negativeComment', '')}".strip()
                    )
                )
                if not text:
                    continue

                all_reviews.append({
                    "reviewer": (
                        r.get("reviewerName")
                        or r.get("displayName")
                        or "Ẩn danh"
                    )[:200],
                    "score":   float(
                        r.get("overallScore")
                        or r.get("rating", 0)
                    ),
                    "text":    text[:3000],
                    "date":    r.get("reviewDate") or r.get("date"),
                    "country": r.get("reviewerCountry") or r.get("country"),
                    "lang":    r.get("languageCode", "en"),
                    "platform":"agoda.com",
                })

            log.info(
                f"    Agoda API trang {page_num}"
                f"/{MAX_PAGES_PER_HOTEL}: "
                f"+{len(reviews)} reviews"
            )
            await asyncio.sleep(
                random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            )

        except Exception as e:
            log.warning(f"    Agoda API trang {page_num} lỗi: {e}")
            break

    return all_reviews


async def _agoda_html_reviews(
        rev_page: Page,
        url:      str,
) -> list[dict]:
    """Fallback: cào review từ HTML."""
    all_reviews: list[dict] = []

    for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
        try:
            await rev_page.goto(
                f"{url}#tab-reviews",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            await asyncio.sleep(
                random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            )
            await human_scroll(rev_page)

            cards = rev_page.locator(
                "[data-selenium='review-item'], "
                "div.Review-comment, "
                "li.review-item"
            )
            batch: list[dict] = []

            for i in range(await cards.count()):
                c = cards.nth(i)
                try:
                    text = await _text(
                        c.locator(
                            "[data-selenium='review-comment'], "
                            "p.review-comment-text"
                        ).first
                    )
                    if not text:
                        continue

                    score_raw = await _text(
                        c.locator(
                            "[data-selenium='review-score'], "
                            "span.review-score-badge"
                        ).first
                    )
                    try:
                        score = float(
                            re.sub(r"[^\d.,]", "", score_raw)
                            .replace(",", ".")
                        )
                    except ValueError:
                        score = 0.0

                    batch.append({
                        "reviewer": clean(await _text(
                            c.locator(
                                "[data-selenium='reviewer-name'], "
                                "span.reviewer-name"
                            ).first
                        )) or "Ẩn danh",
                        "score":    score,
                        "text":     text[:3000],
                        "date":     clean(await _text(
                            c.locator(
                                "[data-selenium='review-date'], "
                                "span.review-date"
                            ).first
                        )),
                        "platform": "agoda.com",
                    })
                except Exception:
                    continue

            if not batch:
                break
            all_reviews.extend(batch)

            next_btn = rev_page.locator(
                "button[data-selenium='next-page'], "
                "a.next-page-link"
            ).first
            if await next_btn.count() == 0:
                break
            await next_btn.click()
            await asyncio.sleep(
                random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            )

        except Exception as e:
            log.warning(f"    Agoda HTML trang {page_num} lỗi: {e}")
            break

    return all_reviews


async def scrape_agoda(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Agoda.com."""
    hotel: dict = {
        "url":      url,
        "platform": "agoda.com",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1.HeaderCugini-module__hotelName",
        "h1[data-selenium='hotel-header-name']",
        "h1.hotel-name",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "span[data-selenium='hotel-address-map']",
        ".hotel-address",
        "[class*='address']",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_els       = page.locator(
        "span[data-selenium='hotel-star-rating'] i, "
        "div.star-rating-container span.icon-ic_star"
    )
    hotel["stars"] = min(await star_els.count(), 5)

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _agoda_parse_rooms(page)

    # ── Reviews ───────────────────────────────────────────────
    hotel_id_m = re.search(r"/hotel/(\d+)", url)
    hotel_id   = hotel_id_m.group(1) if hotel_id_m else None

    rev_page = await open_page(ctx)
    try:
        if hotel_id:
            all_reviews = await _agoda_api_reviews(rev_page, hotel_id)
        else:
            all_reviews = await _agoda_html_reviews(rev_page, url)
    finally:
        await rev_page.close()

    return hotel, rooms, all_reviews