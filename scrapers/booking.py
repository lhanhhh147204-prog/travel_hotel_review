# scrapers/booking.py
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


async def _booking_parse_rooms(page: Page) -> list[dict]:
    rooms = []
    rows  = page.locator(
        "tr.js-rt-block-row, "
        "[data-testid='availability-row'], "
        "div.hprt-table-cell-roomtype"
    )
    for i in range(await rows.count()):
        row = rows.nth(i)
        try:
            name_el = row.locator(
                "span.hprt-roomtype-icon-link, "
                "a.hprt-roomtype-link"
            ).first
            price_el = row.locator(
                "div.bui-price-display__value, "
                "strong.price, "
                "[data-testid='price-and-discounted-price']"
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


async def _booking_parse_reviews(
        rev_page: Page,
        url:      str,
        page_num: int,
) -> list[dict]:
    batch: list[dict] = []
    cards = rev_page.locator(
        "li.review_list_new_item_block, "
        "[data-testid='review-card'], "
        "div.c-review-block"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            pos = await _text(
                c.locator(
                    "p.review_pos, "
                    "[data-testid='review-positive-text'], "
                    "span.c-review__body"
                ).first
            )
            neg = await _text(
                c.locator(
                    "p.review_neg, "
                    "[data-testid='review-negative-text']"
                ).first
            )
            text = f"{pos} {neg}".strip()
            if not text:
                continue

            score_raw = await _text(
                c.locator(
                    "div.bui-review-score__badge, "
                    "[data-testid='review-score']"
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
                        "span.bui-avatar-block__title, "
                        "[data-testid='review-author']"
                    ).first
                )) or "Ẩn danh",
                "score":   score,
                "text":    text[:3000],
                "date":    clean(await _text(
                    c.locator(
                        "span.c-review-block__date, "
                        "[data-testid='review-date']"
                    ).first
                )),
                "country": clean(await _text(
                    c.locator(
                        "span.reviewer_country span, "
                        "[data-testid='review-author-country']"
                    ).first
                )),
                "lang":     "vi",
                "platform": "booking.com",
            })
        except Exception:
            continue
    return batch


async def scrape_booking(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Booking.com."""
    hotel: dict = {
        "url":      url,
        "platform": "booking.com",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h2.pp-header__title",
        "h1.hotel-name",
        "[data-testid='property-name']",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "span.hp_address_subtitle",
        "[data-testid='property-location']",
        ".address span",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_els      = page.locator(
        "span.stars span, "
        "[data-testid='rating-stars'] span"
    )
    hotel["stars"] = min(await star_els.count(), 5)

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _booking_parse_rooms(page)

    # ── Reviews ───────────────────────────────────────────────
    all_reviews: list[dict] = []
    rev_page = await open_page(ctx)
    try:
        for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
            base_url    = url.split("?")[0]
            review_url  = (
                f"{base_url}#tab-reviews"
                if page_num == 1
                else f"{base_url}"
                     f"?offset={(page_num - 1) * 10}"
                     f"#tab-reviews"
            )
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

                batch = await _booking_parse_reviews(
                    rev_page, url, page_num
                )

                if not batch:
                    log.info(
                        f"    Booking: hết review ở trang {page_num}"
                    )
                    break

                all_reviews.extend(batch)
                log.info(
                    f"    Booking trang {page_num}"
                    f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
                )

            except Exception as e:
                log.warning(
                    f"    Booking trang {page_num} lỗi: {e}"
                )
                break
    finally:
        await rev_page.close()

    return hotel, rooms, all_reviews