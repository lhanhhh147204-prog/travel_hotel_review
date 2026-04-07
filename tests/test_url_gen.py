# tests/test_url_gen.py
from __future__ import annotations

import pytest
from core.url_gen import URLGenerator


class TestURLGenerator:
    def test_booking_listing_basic(self):
        urls = URLGenerator.booking_listing("Hà Nội", max_pages=3)
        assert len(urls) == 3
        assert all("booking.com" in u for u in urls)
        assert all("hanoi"       in u for u in urls)

    def test_booking_listing_pagination(self):
        urls = URLGenerator.booking_listing("Hà Nội", max_pages=5)
        offsets = [0, 25, 50, 75, 100]
        for url, offset in zip(urls, offsets):
            assert f"offset={offset}" in url

    def test_agoda_listing_basic(self):
        urls = URLGenerator.agoda_listing("Đà Nẵng", max_pages=3)
        assert len(urls) == 3
        assert all("agoda.com" in u for u in urls)

    def test_tripadvisor_listing_basic(self):
        urls = URLGenerator.tripadvisor_listing(
            "Hà Nội", max_pages=3
        )
        assert len(urls) == 3
        assert all("tripadvisor.com" in u for u in urls)

    def test_tripadvisor_pagination(self):
        urls = URLGenerator.tripadvisor_listing(
            "Hà Nội", max_pages=3
        )
        assert "oa0"  not in urls[0]
        assert "oa30"     in urls[1]
        assert "oa60"     in urls[2]

    def test_google_maps_search(self):
        urls = URLGenerator.google_maps_search("Nha Trang")
        assert len(urls) >= 3
        assert all("google.com/maps" in u for u in urls)
        assert any("khách+sạn" in u or "kh%C3%A1ch" in u
                   for u in urls)

    def test_ivivu_listing(self):
        urls = URLGenerator.ivivu_listing("Phú Quốc", max_pages=3)
        assert len(urls) == 3
        assert all("ivivu.com" in u for u in urls)

    def test_mytour_listing(self):
        urls = URLGenerator.mytour_listing("Đà Lạt", max_pages=3)
        assert len(urls) == 3
        assert all("mytour.vn" in u for u in urls)

    def test_traveloka_listing(self):
        urls = URLGenerator.traveloka_listing(
            "Hội An", max_pages=3
        )
        assert len(urls) == 3
        assert all("traveloka.com" in u for u in urls)

    def test_generate_all_single_province(self):
        result = URLGenerator.generate_all(
            provinces = ["Hà Nội"],
            sources   = ["booking", "agoda"],
            max_pages = 2,
        )
        assert "Hà Nội" in result
        assert len(result["Hà Nội"]) == 4  # 2 booking + 2 agoda

    def test_generate_all_multiple_provinces(self):
        provinces = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng"]
        result    = URLGenerator.generate_all(
            provinces = provinces,
            sources   = ["booking"],
            max_pages = 3,
        )
        assert len(result) == 3
        for province in provinces:
            assert province in result