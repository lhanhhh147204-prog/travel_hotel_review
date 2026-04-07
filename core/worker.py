# core/worker.py
from __future__ import annotations

import asyncio
import logging
import random
import time
from urllib.parse import urlparse

from playwright.async_api import Browser

from config        import CFG
from core.helpers  import (
    make_context, open_page,
    safe_goto, human_scroll,
    DELAY_MIN, DELAY_MAX,
)
from core.dispatcher import extract
from core.state      import ScrapeState
from core.metrics    import PipelineMetrics
from core.throttle   import AdaptiveThrottle
from core.proxy      import ProxyPool
from core.session    import SessionManager
from core.behavior   import HumanBehavior
from db              import save_to_db

log = logging.getLogger(__name__)


class Worker:
    """
    Worker xử lý 1 URL:
    scrape → validate → save DB → update state/metrics.
    """

    def __init__(
            self,
            browser:     Browser,
            sem:         asyncio.Semaphore,
            state:       ScrapeState,
            metrics:     PipelineMetrics,
            throttle:    AdaptiveThrottle,
            proxy_pool:  ProxyPool,
            session_mgr: SessionManager,
            pbar,
    ):
        self.browser     = browser
        self.sem         = sem
        self.state       = state
        self.metrics     = metrics
        self.throttle    = throttle
        self.proxy_pool  = proxy_pool
        self.session_mgr = session_mgr
        self.pbar        = pbar

    async def run(self, url: str) -> None:
        """Xử lý 1 URL — entry point chính."""
        async with self.sem:
            await self._process(url)

    async def _process(self, url: str) -> None:
        t0     = time.time()
        domain = urlparse(url).netloc
        proxy  = await self.proxy_pool.get()
        ctx    = await make_context(self.browser, proxy)
        page   = await open_page(ctx)

        try:
            # ── Load cookie cũ ────────────────────────────────
            await self.session_mgr.load_cookies(ctx, domain)

            # ── Throttle ──────────────────────────────────────
            await self.throttle.wait(domain)

            # ── Điều hướng ────────────────────────────────────
            await safe_goto(page, url)
            await asyncio.sleep(
                random.uniform(DELAY_MIN, DELAY_MAX)
            )

            # ── Human behavior ────────────────────────────────
            await HumanBehavior.simulate_reading(page)
            await HumanBehavior.move_mouse_naturally(page)

            # ── Scrape ────────────────────────────────────────
            hotel, rooms, reviews = await extract(page, url, ctx)

            # ── Validate ──────────────────────────────────────
            if not hotel["name"] or hotel["name"] == "Unknown":
                raise ValueError(
                    f"Không trích được tên khách sạn: {url[:60]}"
                )

            # ── Lưu DB ────────────────────────────────────────
            stats = await asyncio.to_thread(
                save_to_db, hotel, rooms, reviews
            )

            # ── Lưu cookie ────────────────────────────────────
            await self.session_mgr.save_cookies(ctx, domain)

            # ── Cập nhật state & metrics ──────────────────────
            duration = time.time() - t0
            await self.state.mark_ok(url)
            await self.throttle.record_success()
            await self.metrics.record_success(
                url,
                stats["rooms_saved"],
                stats["reviews_saved"],
                duration,
            )

            log.info(
                f"✅  {hotel['name'][:38]:<38} | "
                f"phòng={stats['rooms_saved']:>3} | "
                f"review={stats['reviews_saved']:>4} | "
                f"skip={stats['reviews_skip']:>3} | "
                f"{duration:.1f}s"
            )

        except Exception as exc:
            duration = time.time() - t0
            err_str  = str(exc)[:300]
            is_blocked = any(
                kw in err_str.lower()
                for kw in [
                    "403", "429", "blocked",
                    "captcha", "robot",
                    "access denied",
                ]
            )

            if is_blocked and proxy and proxy.host:
                await self.proxy_pool.mark_bad(proxy)
                await self.session_mgr.rotate_session(ctx, domain)
                log.warning(
                    f"🚫  Proxy {proxy.host} bị block tại {domain}"
                )

            await self.throttle.record_failure(is_blocked)
            await self.state.mark_fail(url, err_str)
            await self.metrics.record_failure(url, exc)

            log.warning(
                f"⚠️  [{url[:55]}]\n"
                f"    Error: {err_str[:100]}"
            )

        finally:
            await ctx.close()
            self.pbar.update(1)


async def process_url(
        url:         str,
        browser:     Browser,
        sem:         asyncio.Semaphore,
        state:       ScrapeState,
        metrics:     PipelineMetrics,
        throttle:    AdaptiveThrottle,
        proxy_pool:  ProxyPool,
        session_mgr: SessionManager,
        pbar,
) -> None:
    """Wrapper function tương thích với pipeline."""
    worker = Worker(
        browser     = browser,
        sem         = sem,
        state       = state,
        metrics     = metrics,
        throttle    = throttle,
        proxy_pool  = proxy_pool,
        session_mgr = session_mgr,
        pbar        = pbar,
    )
    await worker.run(url)