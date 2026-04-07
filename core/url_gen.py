# core/url_gen.py
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote

from playwright.async_api import Page
from core.helpers import _attr, _text

log = logging.getLogger(__name__)


class URLGenerator:
    """Sinh listing URLs cho từng tỉnh thành và nguồn."""

    _BOOKING_SLUGS: dict[str, str] = {
        "Hà Nội":        "hanoi",
        "Hồ Chí Minh":   "ho-chi-minh-city",
        "Đà Nẵng":       "da-nang",
        "Nha Trang":     "nha-trang",
        "Phú Quốc":      "phu-quoc",
        "Đà Lạt":        "da-lat",
        "Hội An":        "hoi-an",
        "Huế":           "hue",
        "Hạ Long":       "ha-long",
        "Sa Pa":         "sapa",
        "Vũng Tàu":      "vung-tau",
        "Mũi Né":        "mui-ne",
        "Quy Nhơn":      "quy-nhon",
        "Cần Thơ":       "can-tho",
        "Hải Phòng":     "hai-phong",
        "Ninh Bình":     "ninh-binh",
        "Buôn Ma Thuột": "buon-ma-thuot",
        "Phan Thiết":    "phan-thiet",
        "Tam Cốc":       "tam-coc",
        "Phong Nha":     "phong-nha",
        "Hà Giang":      "ha-giang",
        "Điện Biên":     "dien-bien",
        "Sơn La":        "son-la",
        "Hòa Bình":      "hoa-binh",
        "Thanh Hóa":     "thanh-hoa",
        "Nghệ An":       "nghe-an",
        "Quảng Bình":    "quang-binh",
        "Quảng Trị":     "quang-tri",
        "Quảng Ngãi":    "quang-ngai",
        "Bình Định":     "binh-dinh",
        "Phú Yên":       "phu-yen",
        "Ninh Thuận":    "ninh-thuan",
        "Bình Thuận":    "binh-thuan",
        "Kon Tum":       "kon-tum",
        "Gia Lai":       "gia-lai",
        "Đắk Lắk":      "dak-lak",
        "Đắk Nông":      "dak-nong",
        "Lâm Đồng":      "lam-dong",
        "Bình Phước":    "binh-phuoc",
        "Tây Ninh":      "tay-ninh",
        "Bình Dương":    "binh-duong",
        "Đồng Nai":      "dong-nai",
        "Bà Rịa-Vũng Tàu": "ba-ria-vung-tau",
        "Long An":       "long-an",
        "Tiền Giang":    "tien-giang",
        "Bến Tre":       "ben-tre",
        "Trà Vinh":      "tra-vinh",
        "Vĩnh Long":     "vinh-long",
        "Đồng Tháp":     "dong-thap",
        "An Giang":      "an-giang",
        "Kiên Giang":    "kien-giang",
        "Hậu Giang":     "hau-giang",
        "Sóc Trăng":     "soc-trang",
        "Bạc Liêu":      "bac-lieu",
        "Cà Mau":        "ca-mau",
        "Bắc Ninh":      "bac-ninh",
        "Bắc Giang":     "bac-giang",
        "Bắc Kạn":       "bac-kan",
        "Cao Bằng":      "cao-bang",
        "Lạng Sơn":      "lang-son",
        "Thái Nguyên":   "thai-nguyen",
        "Phú Thọ":       "phu-tho",
        "Vĩnh Phúc":     "vinh-phuc",
        "Hưng Yên":      "hung-yen",
        "Hải Dương":     "hai-duong",
        "Thái Bình":     "thai-binh",
        "Nam Định":      "nam-dinh",
        "Hà Nam":        "ha-nam",
        "Tuyên Quang":   "tuyen-quang",
        "Yên Bái":       "yen-bai",
        "Lai Châu":      "lai-chau",
        "Lào Cai":       "lao-cai",
        "Khánh Hòa":     "khanh-hoa",
        "Quảng Nam":     "quang-nam",
        "Thừa Thiên Huế":"thua-thien-hue",
        "Quảng Ninh":    "quang-ninh",
    }

    _TA_GEO: dict[str, str] = {
        "Hà Nội":      "g293924",
        "Hồ Chí Minh": "g293925",
        "Đà Nẵng":     "g298085",
        "Hội An":      "g298082",
        "Huế":         "g293926",
        "Nha Trang":   "g298092",
        "Phú Quốc":    "g469404",
        "Đà Lạt":      "g293922",
        "Hạ Long":     "g469418",
        "Sa Pa":       "g311304",
        "Vũng Tàu":    "g303942",
        "Mũi Né":      "g303944",
        "Quy Nhơn":    "g1078753",
        "Cần Thơ":     "g303940",
        "Hải Phòng":   "g303939",
        "Ninh Bình":   "g1544949",
        "Phong Nha":   "g1544950",
        "Hà Giang":    "g1544951",
        "Buôn Ma Thuột":"g1544952",
        "Phan Thiết":  "g303943",
    }

    @classmethod
    def booking_listing(
            cls,
            city:      str,
            checkin:   str = "2025-06-01",
            checkout:  str = "2025-06-02",
            max_pages: int = 5,
    ) -> list[str]:
        slug = cls._BOOKING_SLUGS.get(
            city, city.lower().replace(" ", "-")
        )
        return [
            f"https://www.booking.com/searchresults.vi.html"
            f"?ss={slug}"
            f"&checkin={checkin}"
            f"&checkout={checkout}"
            f"&offset={p * 25}"
            f"&order=review_score_and_count"
            f"&nflt=ht_id%3D204"
            for p in range(max_pages)
        ]

    @classmethod
    def agoda_listing(
            cls,
            city:      str,
            checkin:   str = "2025-06-01",
            checkout:  str = "2025-06-02",
            max_pages: int = 5,
    ) -> list[str]:
        slug = cls._BOOKING_SLUGS.get(
            city, city.lower().replace(" ", "-")
        )
        return [
            f"https://www.agoda.com/vi-vn/city/{slug}.html"
            f"?checkIn={checkin}"
            f"&checkOut={checkout}"
            f"&page={p}"
            f"&sortBy=popularity"
            for p in range(1, max_pages + 1)
        ]

    @classmethod
    def tripadvisor_listing(
            cls,
            city:      str,
            max_pages: int = 5,
    ) -> list[str]:
        geo = cls._TA_GEO.get(city, "g293924")
        urls = []
        for p in range(max_pages):
            offset = p * 30
            suffix = f"-oa{offset}" if offset > 0 else ""
            urls.append(
                f"https://www.tripadvisor.com/Hotels-{geo}"
                f"{suffix}-Hotels.html"
            )
        return urls
    @classmethod
    def google_maps_search(cls, city: str) -> list[str]:
        queries = [
            f"khách sạn {city}",
            f"resort {city}",
            f"homestay {city}",
            f"khu du lịch {city}",
            f"nhà nghỉ {city}",
        ]
        return [
            f"https://www.google.com/maps/search/{quote(q)}"
            for q in queries
        ]

    @classmethod
    def ivivu_listing(
            cls,
            city:      str,
            max_pages: int = 5,
    ) -> list[str]:
        slug = city.lower().replace(" ", "-")
        return [
            f"https://www.ivivu.com/khach-san/{slug}"
            f"?page={p}&sort=review"
            for p in range(1, max_pages + 1)
        ]

    @classmethod
    def mytour_listing(
            cls,
            city:      str,
            max_pages: int = 5,
    ) -> list[str]:
        slug = city.lower().replace(" ", "-")
        return [
            f"https://www.mytour.vn/khach-san/{slug}.html"
            f"?trang={p}&sapxep=diem-danh-gia"
            for p in range(1, max_pages + 1)
        ]

    @classmethod
    def traveloka_listing(
            cls,
            city:      str,
            max_pages: int = 5,
    ) -> list[str]:
        slug = city.lower().replace(" ", "-")
        return [
            f"https://www.traveloka.com/vi-vn/hotel/vietnam/{slug}"
            f"?page={p}&sort=RATING"
            for p in range(1, max_pages + 1)
        ]

    @classmethod
    def generate_all(
            cls,
            provinces: Optional[list[str]] = None,
            sources:   Optional[list[str]] = None,
            max_pages: int = 5,
    ) -> dict[str, list[str]]:
        if provinces is None:
            provinces = list(cls._BOOKING_SLUGS.keys())
        if sources is None:
            sources = [
                "booking", "agoda",
                "tripadvisor", "google_maps",
            ]

        result: dict[str, list[str]] = {}
        for province in provinces:
            urls: list[str] = []
            if "booking"     in sources:
                urls.extend(cls.booking_listing(province, max_pages=max_pages))
            if "agoda"       in sources:
                urls.extend(cls.agoda_listing(province, max_pages=max_pages))
            if "tripadvisor" in sources:
                urls.extend(cls.tripadvisor_listing(province, max_pages=max_pages))
            if "google_maps" in sources:
                urls.extend(cls.google_maps_search(province))
            if "ivivu"       in sources:
                urls.extend(cls.ivivu_listing(province, max_pages=max_pages))
            if "mytour"      in sources:
                urls.extend(cls.mytour_listing(province, max_pages=max_pages))
            if "traveloka"   in sources:
                urls.extend(cls.traveloka_listing(province, max_pages=max_pages))
            result[province] = urls

        return result


class ListingScraper:
    """Trích xuất hotel URLs từ trang listing."""

    @staticmethod
    async def extract_hotel_urls_booking(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "[data-testid='property-card'], "
            "div.sr_property_block, "
            "li[data-testid='property-card-container']"
        )
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link = card.locator(
                    "a[data-testid='title-link'], "
                    "a.hotel_name_link, "
                    "h3 a"
                ).first
                href = await _attr(link, "href")
                if href and "booking.com/hotel" in href:
                    clean_url = href.split("?")[0] + ".vi.html"
                    urls.append(clean_url)
            except Exception:
                continue
        return list(set(urls))

    @staticmethod
    async def extract_hotel_urls_agoda(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "[data-selenium='hotel-item'], "
            "li.hotel-list-item, "
            "div.PropertyCard"
        )
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link = card.locator("a[href*='/hotel/']").first
                href = await _attr(link, "href")
                if href:
                    if not href.startswith("http"):
                        href = f"https://www.agoda.com{href}"
                    urls.append(href.split("?")[0])
            except Exception:
                continue
        return list(set(urls))

    @staticmethod
    async def extract_hotel_urls_tripadvisor(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "div[data-automation='hotel-card-title'] a, "
            "a.property-title, "
            "div.listing_title a"
        )
        for i in range(await cards.count()):
            try:
                href = await _attr(cards.nth(i), "href")
                if href and "Hotel_Review" in href:
                    if not href.startswith("http"):
                        href = f"https://www.tripadvisor.com{href}"
                    urls.append(href.split("?")[0])
            except Exception:
                continue
        return list(set(urls))

    @staticmethod
    async def extract_hotel_urls_ivivu(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "div.hotel-item, "
            "li.property-item, "
            "div.hotel-card"
        )
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link = card.locator("a[href*='/khach-san/']").first
                href = await _attr(link, "href")
                if href:
                    if not href.startswith("http"):
                        href = f"https://www.ivivu.com{href}"
                    urls.append(href.split("?")[0])
            except Exception:
                continue
        return list(set(urls))

    @staticmethod
    async def extract_hotel_urls_mytour(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "div.hotel-item, "
            "li.hotel-list-item, "
            "article.hotel-card"
        )
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link = card.locator("a[href*='/khach-san/']").first
                href = await _attr(link, "href")
                if href:
                    if not href.startswith("http"):
                        href = f"https://www.mytour.vn{href}"
                    urls.append(href.split("?")[0])
            except Exception:
                continue
        return list(set(urls))

    @staticmethod
    async def extract_hotel_urls_traveloka(page: Page) -> list[str]:
        urls  = []
        cards = page.locator(
            "[data-testid='hotel-card'], "
            "div.hotel-list-item, "
            "li.property-card"
        )
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link = card.locator("a[href*='/hotel/']").first
                href = await _attr(link, "href")
                if href:
                    if not href.startswith("http"):
                        href = f"https://www.traveloka.com{href}"
                    urls.append(href.split("?")[0])
            except Exception:
                continue
        return list(set(urls))

    @classmethod
    async def scrape_listing_page(
            cls,
            page:     Page,
            url:      str,
            platform: str,
    ) -> list[str]:
        """Dispatcher cho listing scraper theo platform."""
        extractors = {
            "booking.com":     cls.extract_hotel_urls_booking,
            "agoda.com":       cls.extract_hotel_urls_agoda,
            "tripadvisor.com": cls.extract_hotel_urls_tripadvisor,
            "ivivu.com":       cls.extract_hotel_urls_ivivu,
            "mytour.vn":       cls.extract_hotel_urls_mytour,
            "traveloka.com":   cls.extract_hotel_urls_traveloka,
        }
        for key, extractor in extractors.items():
            if key in platform:
                return await extractor(page)
        return []