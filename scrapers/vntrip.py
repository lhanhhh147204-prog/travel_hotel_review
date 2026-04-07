# scrapers/vntrip.py
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


async def _vntrip_parse_rooms(page: Page) -> list[dict]:
    rooms = []
    rows  = page.locator(
        "div.room-item, "
        "tr.room-row, "
        "li.room-type-item"
    )
    for i in range(await rows.count()):
        row = rows.nth(i)
        try:
            name_el  = row.locator(
                "span.room-name, h3, .room-title"
            ).first
            price_el = row.locator(
                "span.price, div.room-price, "
                ".price-value"
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


async def _vntrip_parse_reviews(page: Page) -> list[dict]:
    batch: list[dict] = []
    cards = page.locator(
        "div.review-item, "
        "li.review, "
        ".review-card"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    "p.review-content, "
                    "div.review-text, "
                    ".comment-content"
                ).first
            )
            if not text:
                continue

            score_raw = await _text(
                c.locator(
                    "span.score, "
                    "div.rating, "
                    ".review-score"
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
                        "span.user-name, "
                        "div.reviewer-name, "
                        ".author"
                    ).first
                )) or "Ẩn danh",
                "score":    score,
                "text":     text[:3000],
                "date":     clean(await _text(
                    c.locator(
                        "span.date, "
                        "time, "
                        ".review-date"
                    ).first
                )),
                "lang":     "vi",
                "platform": "vntrip.vn",
            })
        except Exception:
            continue
    return batch


async def scrape_vntrip(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Vntrip.vn."""
    hotel: dict = {
        "url":      url,
        "platform": "vntrip.vn",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1.hotel-name",
        "h1.property-title",
        ".hotel-info h1",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "span.hotel-address",
        "div.address",
        ".location-text",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_els       = page.locator("i.star, span.icon-star")
    hotel["stars"] = min(await star_els.count(), 5)

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _vntrip_parse_rooms(page)

    # ── Click tab đánh giá ────────────────────────────────────
    for tab_sel in [
        "a[href*='review']",
        "a[href*='danh-gia']",
        "button:has-text('Đánh giá')",
        ".tab-reviews a",
    ]:
        tab = page.locator(tab_sel).first
        if await tab.count() > 0:
            try:
                await tab.click()
                await asyncio.sleep(random.uniform(1.0, 2.0))
                break
            except Exception:
                pass

    # ── Reviews ───────────────────────────────────────────────
    all_reviews: list[dict] = []
    rev_page = await open_page(ctx)

    try:
        for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
            review_url = f"{url}?page={page_num}#reviews"
            try:
                await rev_page.goto(
                    review_url,
                    wait_until="domcontentloaded",
                    timeout=45_000,
                )
                await asyncio.sleep(
                    random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                )
                await human_scroll(rev_page)

                batch = await _vntrip_parse_reviews(rev_page)

                if not batch:
                    log.info(
                        f"    Vntrip: hết review ở trang {page_num}"
                    )
                    break

                all_reviews.extend(batch)
                log.info(
                    f"    Vntrip trang {page_num}"
                    f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
                )

            except Exception as e:
                log.warning(
                    f"    Vntrip trang {page_num} lỗi: {e}"
                )
                break
    finally:
        await rev_page.close()

    return hotel, rooms, all_reviews