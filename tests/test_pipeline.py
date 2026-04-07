# tests/test_pipeline.py
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from core.state   import ScrapeState
from core.metrics import PipelineMetrics
from core.throttle import AdaptiveThrottle


class TestScrapeState:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        ScrapeState.STATE_FILE = tmp_path / "test_state.json"
        self.state = ScrapeState()

    def test_initial_empty(self):
        assert self.state.stats["done"]   == 0
        assert self.state.stats["failed"] == 0

    def test_mark_ok(self):
        asyncio.run(
            self.state.mark_ok("https://example.com/hotel/1")
        )
        assert self.state.stats["done"] == 1

    def test_mark_fail(self):
        asyncio.run(
            self.state.mark_fail(
                "https://example.com/hotel/2",
                "Timeout error"
            )
        )
        assert self.state.stats["failed"] == 1

    def test_should_skip_done(self):
        url = "https://example.com/hotel/3"
        asyncio.run(self.state.mark_ok(url))
        assert self.state.should_skip(url) is True

    def test_should_skip_not_done(self):
        url = "https://example.com/hotel/4"
        assert self.state.should_skip(url) is False

    def test_mark_ok_removes_from_failed(self):
        url = "https://example.com/hotel/5"
        asyncio.run(self.state.mark_fail(url, "Error"))
        assert self.state.stats["failed"] == 1
        asyncio.run(self.state.mark_ok(url))
        assert self.state.stats["failed"] == 0
        assert self.state.stats["done"]   == 1

    def test_persistence(self, tmp_path):
        """Test state được lưu và load lại."""
        url = "https://example.com/hotel/6"
        asyncio.run(self.state.mark_ok(url))

        # Load lại state mới
        state2 = ScrapeState()
        assert state2.should_skip(url) is True


class TestPipelineMetrics:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.metrics = PipelineMetrics(total_urls=100)

    def test_initial_zeros(self):
        assert self.metrics.success_count  == 0
        assert self.metrics.failure_count  == 0
        assert self.metrics.total_rooms    == 0
        assert self.metrics.total_reviews  == 0

    def test_record_success(self):
        asyncio.run(
            self.metrics.record_success(
                url      = "https://example.com",
                rooms    = 5,
                reviews  = 20,
                duration = 10.5,
            )
        )
        assert self.metrics.success_count == 1
        assert self.metrics.total_rooms   == 5
        assert self.metrics.total_reviews == 20

    def test_record_failure(self):
        asyncio.run(
            self.metrics.record_failure(
                url = "https://example.com",
                exc = Exception("Test error"),
            )
        )
        assert self.metrics.failure_count == 1
        assert len(self.metrics.errors)   == 1

    def test_report_format(self):
        report = self.metrics.report()
        assert "PIPELINE METRICS" in report
        assert "Thành công"       in report
        assert "Thất bại"         in report
        assert "Tổng phòng"       in report
        assert "Tổng review"      in report

    def test_multiple_records(self):
        for i in range(5):
            asyncio.run(
                self.metrics.record_success(
                    url      = f"https://example.com/{i}",
                    rooms    = 3,
                    reviews  = 10,
                    duration = 5.0,
                )
            )
        assert self.metrics.success_count == 5
        assert self.metrics.total_rooms   == 15
        assert self.metrics.total_reviews == 50


class TestAdaptiveThrottle:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.throttle = AdaptiveThrottle(
            min_delay   = 0.1,
            max_delay   = 5.0,
            window_size = 10,
        )

    def test_initial_delay(self):
        assert self.throttle.current == 3.0

    def test_record_success_decreases_delay(self):
        initial = self.throttle.current
        for _ in range(10):
            asyncio.run(self.throttle.record_success())
        assert self.throttle.current <= initial

    def test_record_failure_blocked_increases_delay(self):
        initial = self.throttle.current
        asyncio.run(
            self.throttle.record_failure(is_blocked=True)
        )
        assert self.throttle.current > initial

    def test_delay_within_bounds(self):
        for _ in range(20):
            asyncio.run(self.throttle.record_success())
        assert self.throttle.current >= self.throttle.min_delay

        for _ in range(20):
            asyncio.run(
                self.throttle.record_failure(is_blocked=True)
            )
        assert self.throttle.current <= self.throttle.max_delay