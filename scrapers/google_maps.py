# scrapers/google_maps.py
from __future__ import annotations

import asyncio
import random
import re

from playwright.async_api import BrowserContext, Page

from core.helpers import (
    _attr, _first_text, _text,
    clean, open_page, human_scroll,
    MAX_PAGES_PER_HOTEL,
    log,
)


async def _gmaps_click_review_tab(page: Page) -> None:
    """Click vào tab Reviews và sắp xếp mới nhất."""
    for tab_sel in [
        "button[aria-label*='đánh giá']",
        "button[aria-label*='Reviews']",
        "button[data-tab-index='1']",
    ]:
        tab = page.locator(tab_sel).first
        if await tab.count() > 0:
            try:
                await tab.click()
                await asyncio.sleep(random.uniform(1.5, 2.5))
                break
            except Exception:
                pass

    # Sắp xếp mới nhất
    for sort_sel in [
        "button[aria-label*='Sắp xếp']",
        "button[data-value='Sort']",
    ]:
        sort_btn = page.locator(sort_sel).first
        if await sort_btn.count() > 0:
            try:
                await sort_btn.click()
                await asyncio.sleep(0.8)
                for newest_sel in [
                    "li[data-index='1']",
                    "[data-value='newestFirst']",
                ]:
                    newest = page.locator(newest_sel).first
                    if await newest.count() > 0:
                        await newest.click()
                        await asyncio.sleep(1.5)
                        break
                break
            except Exception:
                pass


async def _gmaps_expand_reviews(page: Page) -> None:
    """Bấm 'Xem thêm' để mở rộng nội dung review."""
    more_btns = page.locator(
        "button.w8nwRe, "
        "button[aria-label='Xem thêm'], "
        "button[aria-label='See more']"
    )
    for i in range(await more_btns.count()):
        try:
            await more_btns.nth(i).click()
            await asyncio.sleep(0.3)
        except Exception:
            continue


async def _gmaps_parse_reviews(
        page:       Page,
        prev_count: int,
) -> list[dict]:
    """Parse reviews mới xuất hiện sau khi scroll."""
    batch: list[dict] = []
    cards = page.locator(
        "div.jftiEf.fontBodyMedium, "
        "div[data-review-id], "
        "div.MyEned"
    )
    current_count = await cards.count()

    for i in range(prev_count, current_count):
        c = cards.nth(i)
        try:
            text = await _text(
                c.locator(
                    "span.wiI7pd, "
                    "div.MyEned span"
                ).first
            )
            if not text:
                continue

            score_raw = await _attr(
                c.locator("span.kvMYJc").first,
                "aria-label",
            )
            m     = re.search(r"(\d+)", score_raw or "")
            score = float(m.group(1)) if m else 0.0

            lang_el = c.locator("div.sHQwe span").first
            lang    = (
                await _text(lang_el)
                if await lang_el.count() > 0
                else "vi"
            )

            batch.append({
                "reviewer": clean(await _text(
                    c.locator(
                        "div.d4r55, "
                        "button.WEBjve div.d4r55"
                    ).first
                )) or "Ẩn danh",
                "score":    score,
                "text":     text[:3000],
                "date":     clean(await _text(
                    c.locator(
                        "span.rsqaWe, "
                        "div.DU9Pgb span"
                    ).first
                )),
                "lang":     lang,
                "platform": "google_maps",
            })
        except Exception:
            continue

    return batch, current_count


async def scrape_google_maps(
        page: Page,
        url:  str,
        ctx:  BrowserContext,
) -> tuple[dict, list[dict], list[dict]]:
    """Scraper cho Google Maps."""
    hotel: dict = {
        "url":      url,
        "platform": "google.com/maps",
        "name":     "",
        "address":  "",
        "city":     "Vietnam",
        "stars":    0,
        "type":     "Khách Sạn",
    }

    # ── Tên ──────────────────────────────────────────────────
    hotel["name"] = await _first_text(page, [
        "h1.DUwDvf",
        "h1.fontHeadlineLarge",
        "[data-item-id='title'] h1",
    ]) or "Unknown"

    # ── Địa chỉ ──────────────────────────────────────────────
    hotel["address"] = await _first_text(page, [
        "button[data-item-id='address'] div.fontBodyMedium",
        "[data-tooltip='Sao chép địa chỉ'] div",
        ".rogA2c div",
    ])

    # ── Rating ────────────────────────────────────────────────
    rating_el = page.locator(
        "div.F7nice span[aria-hidden='true']"
    ).first
    try:
        hotel["stars"] = float(
            (await _text(rating_el)).replace(",", ".")
        )
    except ValueError:
        hotel["stars"] = 0

    # ── Click tab Reviews & sort ──────────────────────────────
    await _gmaps_click_review_tab(page)

    # ── Scroll & parse reviews ────────────────────────────────
    all_reviews: list[dict] = []
    prev_count  = 0

    for scroll_round in range(MAX_PAGES_PER_HOTEL):
        # Scroll panel reviews
        await page.evaluate("""
            const panel = document.querySelector(
                'div[aria-label*="đánh giá"], '
                'div.m6QErb.DxyBCb'
            );
            if (panel) panel.scrollTop += 2000;
        """)
        await asyncio.sleep(random.uniform(1.5, 2.5))

        # Expand "Xem thêm"
        await _gmaps_expand_reviews(page)

        # Parse reviews mới
        batch, current_count = await _gmaps_parse_reviews(
            page, prev_count
        )
        all_reviews.extend(batch)

        log.info(
            f"    Google Maps scroll {scroll_round + 1}"
            f"/{MAX_PAGES_PER_HOTEL}: "
            f"tổng {len(all_reviews)} reviews"
        )

        if current_count == prev_count:
            log.info("    Google Maps: không còn review mới")
            break
        prev_count = current_count

    return hotel, [], all_reviews