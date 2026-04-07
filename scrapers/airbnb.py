# scrapers/airbnb.py
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


async def _airbnb_parse_rooms(page: Page) -> list[dict]:
    """Airbnb không có phòng theo kiểu truyền thống."""
    rooms    = []
    price_el = page.locator(
        "span[data-testid='price-summary'], "
        "span._tyxjp1, "
        "[class*='price-items']"
    ).first
    price = parse_price(
        await _text(price_el) if await price_el.count() > 0 else ""
    )
    if price:
        rooms.append({
            "name":      "Toàn bộ chỗ ở",
            "price":     price,
            "available": True,
        })
    return rooms


async def _airbnb_open_all_reviews(page: Page) -> None:
    """Click 'Hiển thị tất cả đánh giá' nếu có."""
    for sel in [
        "button[data-testid='pdp-show-all-reviews-button']",
        "a[href*='reviews']",
        "button:has-text('Hiển thị tất cả')",
        "button:has-text('Show all reviews')",
    ]:
        btn = page.locator(sel).first
        if await btn.count() > 0:
            try:
                await btn.click()
                await asyncio.sleep(2.0)
                break
            except Exception:
                pass


async def _airbnb_parse_reviews(page: Page) -> list[dict]:
    batch: list[dict] = []
    cards = page.locator(
        "div[data-testid='review-card'], "
        "div._1gjypya, "
        "li.l1j9v1wn"
    )
    for i in range(await cards.count()):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    "span[data-testid='review-comment'], "
                    "span._1qsawv5, "
                    ".review-content"
                ).first
            )
            if not text:
                continue

            score_raw = await _text(
                c.locator(
                    "span[data-testid='review-rating'], "
                    "span.r1lutz1s"
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
                        "span[data-testid='review-author'], "
                        "div._1qsawv5 span, "
                        ".reviewer-name"
                    ).first
                )) or "Ẩn danh",
                "score":    score,
                "text":     text[:3000],
                "date":     clean(await _text(
                    c.locator(
                        "span[data-testid='review-date'], "
                        "span._1qsawv5, "
                        ".review-date"
                    ).first
                )),
                "lang":     "en",
                "platform": "airbnb.com",
            })
        except Exception:
            continue
    return batch


async def scrape_airbnb(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Airbnb.com."""
    hotel: dict = {
        "url":      url,
        "platform": "airbnb.com",
        "name":     "",
        "address":  "",
        "city":     extract_city_slug(url),
        "stars":    0,
        "type":     "Homestay",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1[data-testid='listing-title']",
        "h1.hpipapi",
        "h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "span[data-testid='listing-location']",
        "div._9xiloll",
        ".location-text",
    ])

    # ── Rating ────────────────────────────────────────────────
    rating_el = page.locator(
        "span[data-testid='rating-value'], "
        "span.ru0q88m, "
        "[class*='rating-value']"
    ).first
    try:
        hotel["stars"] = float(
            (await _text(rating_el)).replace(",", ".")
        )
    except ValueError:
        hotel["stars"] = 0

    # ── Giá phòng ────────────────────────────────────────────
    rooms = await _airbnb_parse_rooms(page)

    # ── Mở tất cả reviews ────────────────────────────────────
    await _airbnb_open_all_reviews(page)

    # ── Reviews ───────────────────────────────────────────────
    all_reviews: list[dict] = []
    rev_page = await open_page(ctx)

    try:
        await rev_page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=45_000,
        )
        await _airbnb_open_all_reviews(rev_page)

        for page_num in range(1, MAX_PAGES_PER_HOTEL + 1):
            await asyncio.sleep(
                random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            )
            await human_scroll(rev_page)

            batch = await _airbnb_parse_reviews(rev_page)

            if not batch:
                log.info(
                    f"    Airbnb: hết review ở trang {page_num}"
                )
                break

            all_reviews.extend(batch)
            log.info(
                f"    Airbnb trang {page_num}"
                f"/{MAX_PAGES_PER_HOTEL}: +{len(batch)} reviews"
            )

            # ── Tìm nút next ──────────────────────────────────
            next_found = False
            for next_sel in [
                "button[aria-label='Next']",
                "a[aria-label='Next page']",
                "button:has-text('Tiếp theo')",
                "[data-testid='pagination-next']",
            ]:
                btn = rev_page.locator(next_sel).first
                if await btn.count() > 0:
                    try:
                        await btn.click()
                        await asyncio.sleep(
                            random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                        )
                        next_found = True
                        break
                    except Exception:
                        pass

            if not next_found:
                break
    finally:
        await rev_page.close()

    return hotel, rooms, all_reviews