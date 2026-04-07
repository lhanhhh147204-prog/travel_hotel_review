# core/throttle.py
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque

log = logging.getLogger(__name__)


class AdaptiveThrottle:
    """Tự động điều chỉnh tốc độ scrape theo tỷ lệ thành công."""

    def __init__(
            self,
            min_delay:   float = 1.0,
            max_delay:   float = 15.0,
            window_size: int   = 20,
    ):
        self.min_delay   = min_delay
        self.max_delay   = max_delay
        self.current     = 3.0
        self._history    = deque(maxlen=window_size)
        self._lock       = asyncio.Lock()
        self._domain_last: dict[str, float] = {}

    async def wait(self, domain: str) -> None:
        async with self._lock:
            now  = time.time()
            last = self._domain_last.get(domain, 0)
            wait = self.current - (now - last)
            if wait > 0:
                await asyncio.sleep(wait + random.uniform(0, 1))
            self._domain_last[domain] = time.time()

    async def record_success(self) -> None:
        async with self._lock:
            self._history.append(True)
            self._adjust()

    async def record_failure(self, is_blocked: bool = False) -> None:
        async with self._lock:
            self._history.append(False)
            if is_blocked:
                self.current = min(self.current * 2.5, self.max_delay)
                log.warning(
                    f"⚠️  Bị block! Tăng delay → {self.current:.1f}s"
                )
            else:
                self._adjust()

    def _adjust(self) -> None:
        if len(self._history) < 5:
            return
        rate = sum(self._history) / len(self._history)
        if rate > 0.9:
            self.current = max(self.current * 0.85, self.min_delay)
        elif rate < 0.6:
            self.current = min(self.current * 1.5, self.max_delay)
        log.debug(
            f"Throttle: rate={rate:.0%} delay={self.current:.1f}s"
        )