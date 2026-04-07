# tests/conftest.py
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from playwright.async_api import Page, BrowserContext


# ── Fixtures dùng chung ───────────────────────────────────────

@pytest.fixture(scope="session")
def event_loop():
    """Tạo event loop dùng chung cho toàn bộ test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_page():
    """Mock Playwright Page object."""
    page = AsyncMock(spec=Page)
    page.viewport_size = {"width": 1366, "height": 768}

    # ── Locator mock ──────────────────────────────────────────
    def make_locator(text="", count=0):
        loc = AsyncMock()
        loc.count         = AsyncMock(return_value=count)
        loc.inner_text    = AsyncMock(return_value=text)
        loc.get_attribute = AsyncMock(return_value="")
        loc.first         = loc
        loc.nth           = MagicMock(return_value=loc)
        loc.click         = AsyncMock()
        return loc

    page.locator   = MagicMock(side_effect=lambda sel: make_locator())
    page.goto      = AsyncMock()
    page.content   = AsyncMock(return_value="<html></html>")
    page.evaluate  = AsyncMock()
    page.mouse     = AsyncMock()
    page.keyboard  = AsyncMock()
    page.route     = AsyncMock()
    page.on        = MagicMock()
    return page


@pytest.fixture
def mock_context():
    """Mock Playwright BrowserContext object."""
    ctx = AsyncMock(spec=BrowserContext)
    ctx.new_page      = AsyncMock()
    ctx.add_cookies   = AsyncMock()
    ctx.cookies       = AsyncMock(return_value=[])
    ctx.clear_cookies = AsyncMock()
    ctx.add_init_script = AsyncMock()
    ctx.close         = AsyncMock()
    return ctx


@pytest.fixture
def mock_page_with_hotel(mock_page):
    """Mock page với dữ liệu khách sạn mẫu."""
    def make_locator_with_text(text="", count=1):
        loc = AsyncMock()
        loc.count         = AsyncMock(return_value=count)
        loc.inner_text    = AsyncMock(return_value=text)
        loc.get_attribute = AsyncMock(return_value="")
        loc.first         = loc
        loc.nth           = MagicMock(return_value=loc)
        loc.click         = AsyncMock()
        return loc

    hotel_data = {
        "h2.pp-header__title":                  "Khách Sạn Test",
        "span.hp_address_subtitle":             "123 Đường Test, Hà Nội",
        "span.stars span":                      "",
        "tr.js-rt-block-row":                   "",
        "li.review_list_new_item_block":        "",
    }

    def locator_side_effect(selector):
        text = hotel_data.get(selector, "")
        return make_locator_with_text(text)

    mock_page.locator = MagicMock(
        side_effect=locator_side_effect
    )
    return mock_page


@pytest.fixture
def sample_hotel() -> dict:
    return {
        "url":      "https://www.booking.com/hotel/vn/test.html",
        "platform": "booking.com",
        "name":     "Khách Sạn Test Hà Nội",
        "address":  "123 Phố Test, Hoàn Kiếm, Hà Nội",
        "city":     "Hà Nội",
        "stars":    4,
        "type":     "Khách Sạn",
    }


@pytest.fixture
def sample_rooms() -> list[dict]:
    return [
        {
            "name":      "Phòng Đơn Tiêu Chuẩn",
            "price":     450_000.0,
            "available": True,
        },
        {
            "name":      "Phòng Đôi Deluxe",
            "price":     750_000.0,
            "available": True,
        },
        {
            "name":      "Phòng Suite",
            "price":     1_500_000.0,
            "available": False,
        },
    ]


@pytest.fixture
def sample_reviews() -> list[dict]:
    return [
        {
            "reviewer": "Nguyễn Văn A",
            "score":    9.0,
            "text":     "Khách sạn rất tuyệt vời, phòng sạch sẽ và thoáng mát",
            "date":     "2024-03-15",
            "country":  "Việt Nam",
            "lang":     "vi",
            "platform": "booking.com",
        },
        {
            "reviewer": "John Smith",
            "score":    8.5,
            "text":     "Excellent hotel, very clean and friendly staff",
            "date":     "2024-03-10",
            "country":  "United Kingdom",
            "lang":     "en",
            "platform": "booking.com",
        },
        {
            "reviewer": "김민준",
            "score":    7.0,
            "text":     "호텔이 좋았습니다. 위치가 편리해요",
            "date":     "2024-03-05",
            "country":  "South Korea",
            "lang":     "ko",
            "platform": "booking.com",
        },
        {
            "reviewer": "Ẩn danh",
            "score":    3.0,
            "text":     "Phòng bẩn, dịch vụ tệ, rất thất vọng",
            "date":     "2024-02-20",
            "country":  "Việt Nam",
            "lang":     "vi",
            "platform": "booking.com",
        },
    ]