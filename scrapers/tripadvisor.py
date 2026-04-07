# scrapers/tripadvisor.py
from __future__ import annotations

import asyncio
import random
import re

from playwright.async_api import BrowserContext, Page

from core.helpers import (
    _attr, _first_text, _text,
    clean, extract_city_slug,
    open_page, human_scroll,
    parse_price,
    PAGE_DELAY_MIN, PAGE_DELAY_MAX,
    MAX_PAGES_PER_HOTEL,
    log,
)


async def _ta_parse_rooms(page: Page) -> list[dict]:
    rooms    = []
    price_els = page.locator(
        "[data-automation='hotel-price'], "
        ".prw_rup.prw_meta_hsx_responsive_price, "
        "span.price"
    )
    for i in range(await price_els.count()):
        p = parse_price(await _text(price_els.nth(i)))
        if p:
            rooms.append({
                "name":      "Phòng Tiêu Chuẩn",
                "price":     p,
                "available": True,
            })
    return rooms


async def _ta_parse_reviews(rev_page: Page) -> list[dict]:
    batch: list[dict] = []
    cards = rev_page.locator(
        "div[data-automation='reviewCard'], "
        ".review-container, "
        "[class*='reviewSelector']"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    ".partial_entry, "
                    "[data-automation='reviewBody'], "
                    "q.QewHA"
                ).first
            )
            if not text:
                continue

            score_raw = await _attr(
                c.locator(
                    "span[class*='ui_bubble_rating']"
                ).first,
                "class",
            )
            m2    = re.search(r"bubble_(\d+)", score_raw)
            score = int(m2.group(1)) / 10 if m2 else 0.0

            batch.append({
                "reviewer": clean(await _text(
                    c.locator(
                        ".info_text div, "
                        "[data-automation='reviewerName']"
                    ).first
                )) or "Ẩn danh",
                "score":   score,
                "text":    text[:3000],
                "date":    clean(await _text(
                    c.locator(
                        ".ratingDate, "
                        "[data-automation='reviewDate']"
                    ).first
                )),
                "title":   clean(await _text(
                    c.locator(
                        ".noQuotes, "
                        "[data-automation='reviewTitle']"
                    ).first
                )),
                "country": clean(await _text(
                    c.locator(".userLoc strong").first
                )),
                "lang":     "en",
                "platform": "tripadvisor.com",
            })
        except Exception:
            continue
    return batch


async def scrape_tripadvisor(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho TripAdvisor.com."""
    hotel: dict = {
        "url":      url,
        "platform": "tripadvisor.com",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1.QdLfr",
        "h1[data-automation='mainH1']",
        ".fIrGe h1",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "span.biGQs._P.pZUbB.hmDzD",
        "[data-automation='hotel-address']",
        ".fHvkI span",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_el  = page.locator(
        ".ui_star_rating span[class*='ui_star_rating']"
    )
    # ── Số sao────────────────────────────────────────
    raw_cls        = await _attr(star_el.first, "class")
    m              = re.search(r"bubble_(\d+)", raw_cls)
    hotel["stars"] = int(m.group(1)[0]) if m else 0

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _ta_parse_rooms(page)

    # ── Reviews ───────────────────────────────────────────────
    all_reviews: list[dict] = []
    base     = re.sub(r"-or\d+", "", url.split("?")[0])
    rev_page = await open_page(ctx)

    try:
        for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
            offset   = (page_num - 1) * 10
            page_url = (
                re.sub(r"(Reviews)", f"Reviews-or{offset}", base)
                if offset > 0
                else base
            )
            try:
                await rev_page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=45_000,
                )
                await asyncio.sleep(
                    random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                )
                await human_scroll(rev_page)

                batch = await _ta_parse_reviews(rev_page)

                if not batch:
                    log.info(
                        f"    TripAdvisor: hết review "
                        f"ở trang {page_num}"
                    )
                    break

                all_reviews.extend(batch)
                log.info(
                    f"    TripAdvisor trang {page_num}"
                    f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
                )

            except Exception as e:
                log.warning(
                    f"    TripAdvisor trang {page_num} lỗi: {e}"
                )
                break
    finally:
        await rev_page.close()

    return hotel, rooms, all_reviews