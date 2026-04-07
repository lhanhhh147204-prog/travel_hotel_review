# tests/test_scrapers.py
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestBookingScraper:
    @pytest.mark.asyncio
    async def test_scrape_booking_returns_tuple(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "scrapers.booking.open_page",
            return_value=mock_page,
        ):
            from scrapers.booking import scrape_booking
            hotel, rooms, reviews = await scrape_booking(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            assert isinstance(hotel,   dict)
            assert isinstance(rooms,   list)
            assert isinstance(reviews, list)

    @pytest.mark.asyncio
    async def test_scrape_booking_hotel_fields(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "scrapers.booking.open_page",
            return_value=mock_page,
        ):
            from scrapers.booking import scrape_booking
            hotel, _, _ = await scrape_booking(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            assert "url"      in hotel
            assert "platform" in hotel
            assert "name"     in hotel
            assert "city"     in hotel
            assert "stars"    in hotel
            assert hotel["platform"] == "booking.com"

    @pytest.mark.asyncio
    async def test_scrape_booking_page_closed(
            self,
            mock_page,
            mock_context,
    ):
        """Rev page phải được đóng sau khi scrape."""
        rev_page = AsyncMock()
        rev_page.count         = AsyncMock(return_value=0)
        rev_page.goto          = AsyncMock()
        rev_page.evaluate      = AsyncMock()
        rev_page.route         = AsyncMock()
        rev_page.close         = AsyncMock()
        rev_page.locator       = MagicMock(
            return_value=AsyncMock(
                count=AsyncMock(return_value=0),
                first=AsyncMock(),
            )
        )

        with patch(
            "scrapers.booking.open_page",
            return_value=rev_page,
        ):
            from scrapers.booking import scrape_booking
            await scrape_booking(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            rev_page.close.assert_called_once()


class TestAgodaScraper:
    @pytest.mark.asyncio
    async def test_scrape_agoda_returns_tuple(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "scrapers.agoda.open_page",
            return_value=mock_page,
        ):
            from scrapers.agoda import scrape_agoda
            hotel, rooms, reviews = await scrape_agoda(
                mock_page,
                "https://www.agoda.com/hotel/12345.html",
                mock_context,
            )
            assert isinstance(hotel,   dict)
            assert isinstance(rooms,   list)
            assert isinstance(reviews, list)
            assert hotel["platform"] == "agoda.com"

    @pytest.mark.asyncio
    async def test_scrape_agoda_with_hotel_id(
            self,
            mock_page,
            mock_context,
    ):
        """Test khi URL có hotel ID → dùng API."""
        rev_page = AsyncMock()
        rev_page.goto    = AsyncMock()
        rev_page.content = AsyncMock(
            return_value='{"reviewList": []}'
        )
        rev_page.route   = AsyncMock()
        rev_page.close   = AsyncMock()
        rev_page.locator = MagicMock(
            return_value=AsyncMock(
                count=AsyncMock(return_value=0),
                first=AsyncMock(),
            )
        )

        with patch(
            "scrapers.agoda.open_page",
            return_value=rev_page,
        ):
            from scrapers.agoda import scrape_agoda
            hotel, rooms, reviews = await scrape_agoda(
                mock_page,
                "https://www.agoda.com/hotel/12345.html",
                mock_context,
            )
            assert hotel["platform"] == "agoda.com"


class TestTripAdvisorScraper:
    @pytest.mark.asyncio
    async def test_scrape_tripadvisor_returns_tuple(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "scrapers.tripadvisor.open_page",
            return_value=mock_page,
        ):
            from scrapers.tripadvisor import scrape_tripadvisor
            hotel, rooms, reviews = await scrape_tripadvisor(
                mock_page,
                "https://www.tripadvisor.com/Hotel_Review-g293924.html",
                mock_context,
            )
            assert isinstance(hotel,   dict)
            assert isinstance(rooms,   list)
            assert isinstance(reviews, list)
            assert hotel["platform"] == "tripadvisor.com"


class TestGoogleMapsScraper:
    @pytest.mark.asyncio
    async def test_scrape_google_maps_returns_tuple(
            self,
            mock_page,
            mock_context,
    ):
        from scrapers.google_maps import scrape_google_maps
        hotel, rooms, reviews = await scrape_google_maps(
            mock_page,
            "https://www.google.com/maps/place/Test+Hotel",
            mock_context,
        )
        assert isinstance(hotel,   dict)
        assert isinstance(rooms,   list)
        assert isinstance(reviews, list)
        assert hotel["platform"] == "google.com/maps"
        assert rooms == []  # Google Maps không có rooms


class TestDispatcher:
    @pytest.mark.asyncio
    async def test_dispatch_booking(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "core.dispatcher.scrape_booking",
            new_callable=AsyncMock,
        ) as mock_scraper:
            mock_scraper.return_value = (
                {
                    "url":      "https://booking.com/hotel/test",
                    "platform": "booking.com",
                    "name":     "Test Hotel",
                    "city":     "Hà Nội",
                    "stars":    4,
                },
                [{"name": "Phòng Đôi", "price": 500_000}],
                [{"text": "Rất tốt",   "score": 9.0}],
            )
            from core.dispatcher import extract
            hotel, rooms, reviews = await extract(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            assert hotel["platform"] == "booking.com"
            mock_scraper.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_unknown_domain(
            self,
            mock_page,
            mock_context,
    ):
        from core.dispatcher import extract
        hotel, rooms, reviews = await extract(
            mock_page,
            "https://unknown-domain.com/hotel/test",
            mock_context,
        )
        assert rooms   == []
        assert reviews == []

    @pytest.mark.asyncio
    async def test_dispatch_filters_invalid_rooms(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "core.dispatcher.scrape_booking",
            new_callable=AsyncMock,
        ) as mock_scraper:
            mock_scraper.return_value = (
                {
                    "url":      "https://booking.com/hotel/test",
                    "platform": "booking.com",
                    "name":     "Test Hotel",
                    "city":     "Hà Nội",
                    "stars":    4,
                },
                [
                    {"name": "Phòng Đôi", "price": 500_000},  # valid
                    {"name": "",          "price": 0},          # invalid
                    {"name": "Phòng Đơn", "price": None},       # invalid
                ],
                [],
            )
            from core.dispatcher import extract
            _, rooms, _ = await extract(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            assert len(rooms) == 1

    @pytest.mark.asyncio
    async def test_dispatch_filters_short_reviews(
            self,
            mock_page,
            mock_context,
    ):
        with patch(
            "core.dispatcher.scrape_booking",
            new_callable=AsyncMock,
        ) as mock_scraper:
            mock_scraper.return_value = (
                {
                    "url":      "https://booking.com/hotel/test",
                    "platform": "booking.com",
                    # test_dispatch_filters_short_reviews (tiếp)
                    "name": "Test Hotel",
                    "city": "Hà Nội",
                    "stars": 4,
                },
                [],
                [
                    {"text": "Tốt", "score": 9.0},  # quá ngắn < 10 ký tự
                    {"text": "", "score": 8.0},  # rỗng
                    {"text": "Khách sạn rất tốt, phòng sạch sẽ",
                     "score": 9.0},  # hợp lệ
                ],
            )
            from core.dispatcher import extract
            _, _, reviews = await extract(
                mock_page,
                "https://www.booking.com/hotel/vn/test.html",
                mock_context,
            )
            assert len(reviews) == 1

        class TestWorker:
            @pytest.mark.asyncio
            async def test_worker_success_flow(
                    self,
                    mock_page,
                    mock_context,
            ):
                from core.state import ScrapeState
                from core.metrics import PipelineMetrics
                from core.throttle import AdaptiveThrottle
                from core.proxy import ProxyPool, ProxyConfig
                from core.session import SessionManager
                from core.worker import Worker

                state = MagicMock(spec=ScrapeState)
                state.should_skip = MagicMock(return_value=False)
                state.mark_ok = AsyncMock()
                state.mark_fail = AsyncMock()

                metrics = MagicMock(spec=PipelineMetrics)
                metrics.record_success = AsyncMock()
                metrics.record_failure = AsyncMock()

                throttle = MagicMock(spec=AdaptiveThrottle)
                throttle.wait = AsyncMock()
                throttle.record_success = AsyncMock()
                throttle.record_failure = AsyncMock()

                proxy_pool = MagicMock(spec=ProxyPool)
                proxy_pool.get = AsyncMock(return_value=None)
                proxy_pool.mark_bad = AsyncMock()

                session_mgr = MagicMock(spec=SessionManager)
                session_mgr.load_cookies = AsyncMock(return_value=False)
                session_mgr.save_cookies = AsyncMock()
                session_mgr.rotate_session = AsyncMock()

                pbar = MagicMock()
                pbar.update = MagicMock()

                mock_browser = AsyncMock()
                mock_browser.new_context = AsyncMock(
                    return_value=mock_context
                )
                mock_context.new_page = AsyncMock(return_value=mock_page)
                mock_page.route = AsyncMock()

                with patch(
                        "core.worker.safe_goto",
                        new_callable=AsyncMock,
                ), patch(
                    "core.worker.extract",
                    new_callable=AsyncMock,
                    return_value=(
                            {
                                "url": "https://booking.com/hotel/test",
                                "platform": "booking.com",
                                "name": "Test Hotel",
                                "city": "Hà Nội",
                                "stars": 4,
                            },
                            [{"name": "Phòng Đôi", "price": 500_000}],
                            [{"text": "Rất tốt lắm ạ", "score": 9.0}],
                    ),
                ), patch(
                    "core.worker.save_to_db",
                    return_value={
                        "rooms_saved": 1,
                        "reviews_saved": 1,
                        "reviews_skip": 0,
                    },
                ), patch(
                    "core.worker.HumanBehavior.simulate_reading",
                    new_callable=AsyncMock,
                ), patch(
                    "core.worker.HumanBehavior.move_mouse_naturally",
                    new_callable=AsyncMock,
                ):
                    worker = Worker(
                        browser=mock_browser,
                        sem=asyncio.Semaphore(1),
                        state=state,
                        metrics=metrics,
                        throttle=throttle,
                        proxy_pool=proxy_pool,
                        session_mgr=session_mgr,
                        pbar=pbar,
                    )
                    await worker.run(
                        "https://www.booking.com/hotel/vn/test.html"
                    )

                    state.mark_ok.assert_called_once()
                    metrics.record_success.assert_called_once()
                    pbar.update.assert_called_once_with(1)

            @pytest.mark.asyncio
            async def test_worker_failure_flow(
                    self,
                    mock_page,
                    mock_context,
            ):
                from core.state import ScrapeState
                from core.metrics import PipelineMetrics
                from core.throttle import AdaptiveThrottle
                from core.proxy import ProxyPool
                from core.session import SessionManager
                from core.worker import Worker

                state = MagicMock(spec=ScrapeState)
                state.should_skip = MagicMock(return_value=False)
                state.mark_ok = AsyncMock()
                state.mark_fail = AsyncMock()

                metrics = MagicMock(spec=PipelineMetrics)
                metrics.record_success = AsyncMock()
                metrics.record_failure = AsyncMock()

                throttle = MagicMock(spec=AdaptiveThrottle)
                throttle.wait = AsyncMock()
                throttle.record_success = AsyncMock()
                throttle.record_failure = AsyncMock()

                proxy_pool = MagicMock(spec=ProxyPool)
                proxy_pool.get = AsyncMock(return_value=None)
                proxy_pool.mark_bad = AsyncMock()

                session_mgr = MagicMock(spec=SessionManager)
                session_mgr.load_cookies = AsyncMock(return_value=False)
                session_mgr.save_cookies = AsyncMock()
                session_mgr.rotate_session = AsyncMock()

                pbar = MagicMock()
                pbar.update = MagicMock()

                mock_browser = AsyncMock()
                mock_browser.new_context = AsyncMock(
                    return_value=mock_context
                )
                mock_context.new_page = AsyncMock(return_value=mock_page)
                mock_page.route = AsyncMock()

                with patch(
                        "core.worker.safe_goto",
                        side_effect=Exception("Connection timeout"),
                ):
                    worker = Worker(
                        browser=mock_browser,
                        sem=asyncio.Semaphore(1),
                        state=state,
                        metrics=metrics,
                        throttle=throttle,
                        proxy_pool=proxy_pool,
                        session_mgr=session_mgr,
                        pbar=pbar,
                    )
                    await worker.run(
                        "https://www.booking.com/hotel/vn/test.html"
                    )

                    state.mark_fail.assert_called_once()
                    metrics.record_failure.assert_called_once()
                    pbar.update.assert_called_once_with(1)