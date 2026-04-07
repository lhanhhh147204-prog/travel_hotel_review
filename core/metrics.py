# core/metrics.py
from __future__ import annotations

import asyncio
import time
import logging

log = logging.getLogger(__name__)


class PipelineMetrics:
    """Thu thập và báo cáo metrics của pipeline."""

    def __init__(self, total_urls: int = 0):
        self._lock          = asyncio.Lock()
        self.total_urls     = total_urls
        self.success_count  = 0
        self.failure_count  = 0
        self.total_rooms    = 0
        self.total_reviews  = 0
        self.total_duration = 0.0
        self.errors:  list[str] = []
        self._start   = time.time()

    async def record_success(
            self,
            url:      str,
            rooms:    int,
            reviews:  int,
            duration: float,
    ) -> None:
        async with self._lock:
            self.success_count  += 1
            self.total_rooms    += rooms
            self.total_reviews  += reviews
            self.total_duration += duration

    async def record_failure(
            self,
            url: str,
            exc: Exception,
    ) -> None:
        async with self._lock:
            self.failure_count += 1
            self.errors.append(
                f"{url[:60]}: {str(exc)[:100]}"
            )

    def report(self) -> str:
        elapsed = time.time() - self._start
        total   = self.success_count + self.failure_count
        rate    = self.success_count / max(total, 1) * 100
        avg_t   = self.total_duration / max(self.success_count, 1)

        return (
            f"\n{'═' * 60}\n"
            f"📊  PIPELINE METRICS\n"
            f"{'═' * 60}\n"
            f"  Tổng xử lý    : {total:>6,}\n"
            f"  Thành công    : {self.success_count:>6,} ({rate:.1f}%)\n"
            f"  Thất bại      : {self.failure_count:>6,}\n"
            f"  Tổng phòng    : {self.total_rooms:>6,}\n"
            f"  Tổng review   : {self.total_reviews:>6,}\n"
            f"  Thời gian TB  : {avg_t:>6.1f}s/KS\n"
            f"  Tổng thời gian: {elapsed / 3600:.1f}h\n"
            f"{'═' * 60}"
        )