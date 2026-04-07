# scrapers/mytour.py
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


async def _mytour_parse_rooms(page: Page) -> list[dict]:
    rooms = []
    rows  = page.locator(
        "div.room-type-item, "
        "tr.room-item, "
        "li.room-list-item"
    )
    for i in range(await rows.count()):
        row = rows.nth(i)
        try:
            name_el  = row.locator(
                "h3.room-name, span.room-title"
            ).first
            price_el = row.locator(
                "span.price, div.room-price strong"
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

    # ── Fallback nếu không tìm được phòng ────────────────────
    if not rooms:
        for sel in [".price-value", ".room-price", "[class*='price']"]:
            els = page.locator(sel)
            for i in range(await els.count()):
                p = parse_price(await _text(els.nth(i)))
                if p:
                    rooms.append({
                        "name":      "Phòng Tiêu Chuẩn",
                        "price":     p,
                        "available": True,
                    })
            if rooms:
                break

    return rooms


async def _mytour_parse_reviews(page: Page) -> list[dict]:
    batch: list[dict] = []
    cards = page.locator(
        "div.review-item, "
        "li.comment-item, "
        "[class*='review-card'], "
        ".user-review"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    "p.review-content, "
                    "div.comment-content, "
                    ".review-text, p"
                ).first
            )
            if not text:
                continue

            score_raw = await _text(
                c.locator(
                    ".review-score, .rating-point, "
                    "[class*='score'], .diem-danh-gia"
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
                        ".reviewer-name, .user-name, "
                        "strong.name, .author-name"
                    ).first
                )) or "Ẩn danh",
                "score":    score,
                "text":     text[:3000],
                "date":     clean(await _text(
                    c.locator(
                        ".review-date, .comment-date, "
                        "time, .ngay-danh-gia"
                    ).first
                )),
                "title":    clean(await _text(
                    c.locator(".review-title, h4, h5").first
                )),
                "lang":     "vi",
                "platform": "mytour.vn",
            })
        except Exception:
            continue
    return batch


async def scrape_mytour(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Mytour.vn."""
    hotel: dict = {
        "url":      url,
        "platform": "mytour.vn",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1.hotel-name",
        "h1.title-hotel",
        ".hotel-detail-name h1",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        ".hotel-address",
        ".address-detail",
        "[class*='address']",
        ".dia-chi",
    ])

    # ── Số sao ────────────────────────────────────────────────
    star_els       = page.locator(
        ".star-rating i.active, "
        ".hotel-star span.active, "
        "i.icon-star-active"
    )
    hotel["stars"] = min(await star_els.count(), 5)

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _mytour_parse_rooms(page)

    # ── Click tab đánh giá ────────────────────────────────────
    for tab_sel in [
        "a[href*='danh-gia']",
        "a[href*='review']",
        ".tab-review a",
        "li:has-text('Đánh giá') a",
        "button:has-text('Đánh giá')",
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

    for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
        batch = await _mytour_parse_reviews(page)
        if not batch:
            log.info(f"    Mytour: hết review ở trang {page_num}")
            break

        all_reviews.extend(batch)
        log.info(
            f"    Mytour trang {page_num}"
            f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
        )

        # ── Tìm nút next ──────────────────────────────────────
        next_found = False
        for next_sel in [
            "a.next-page",
            "button.next-page",
            ".pagination .next a",
            "a[rel='next']",
            "button:has-text('Tiếp theo')",
            "a:has-text('Tiếp')",
            ".btn-xem-them-danh-gia",
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