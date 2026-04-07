# tests/test_helpers.py
from __future__ import annotations

import pytest
from core.helpers import (
    clean,
    parse_price,
    parse_date,
    extract_city_slug,
    get_region,
)


class TestClean:
    def test_basic(self):
        assert clean("  hello world  ") == "hello world"

    def test_control_chars(self):
        assert clean("hello\x00world") == "hello world"

    def test_none(self):
        assert clean(None) is None

    def test_empty(self):
        assert clean("") is None

    def test_max_len(self):
        s = "a" * 600
        assert len(clean(s, max_len=500)) == 500

    def test_multiple_spaces(self):
        assert clean("hello   world") == "hello world"


class TestParsePrice:
    def test_vnd_dot_format(self):
        assert parse_price("1.200.000 ₫") == 1_200_000.0

    def test_vnd_comma_format(self):
        assert parse_price("1,200,000 VND") == 1_200_000.0

    def test_usd_format(self):
        result = parse_price("$50")
        assert result == 50 * 24_000

    def test_plain_number(self):
        assert parse_price("500000") == 500_000.0

    def test_none_input(self):
        assert parse_price(None) is None

    def test_empty_input(self):
        assert parse_price("") is None

    def test_invalid_input(self):
        assert parse_price("abc") is None

    def test_too_small(self):
        assert parse_price("50") is None

    def test_too_large(self):
        assert parse_price("999999999999") is None


class TestParseDate:
    def test_iso_format(self):
        assert parse_date("2024-03-15") == "2024-03-15"

    def test_slash_format(self):
        assert parse_date("15/03/2024") == "2024-03-15"

    def test_english_month(self):
        assert parse_date("March 2024") == "2024-03-01"

    def test_english_month_short(self):
        assert parse_date("Mar 2024") == "2024-03-01"

    def test_vietnamese_month(self):
        result = parse_date("Tháng 3 năm 2024")
        assert result == "2024-03-01"

    def test_full_english_date(self):
        assert parse_date("15 March 2024") == "2024-03-15"

    def test_none_input(self):
        assert parse_date(None) is None

    def test_empty_input(self):
        assert parse_date("") is None

    def test_invalid_input(self):
        assert parse_date("not a date") is None


class TestExtractCitySlug:
    def test_booking_url(self):
        url = "https://www.booking.com/hotel/vn/hanoi-hotel.html"
        result = extract_city_slug(url)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_unknown_url(self):
        result = extract_city_slug("https://example.com")
        assert result == "Vietnam"

    def test_normalized_city(self):
        url = "https://www.booking.com/searchresults.html?ss=ha-noi"
        result = extract_city_slug(url)
        assert isinstance(result, str)


class TestGetRegion:
    def test_hanoi(self):
        assert get_region("Hà Nội") == "Miền Bắc"

    def test_hcm(self):
        assert get_region("Hồ Chí Minh") == "Miền Nam"

    def test_danang(self):
        assert get_region("Đà Nẵng") == "Miền Trung"

    def test_unknown(self):
        assert get_region("Unknown City") == "Miền Nam"