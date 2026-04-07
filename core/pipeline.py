# core/pipeline.py
from __future__ import annotations

import asyncio
import logging
import random
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from tqdm import tqdm

from config import CFG
from core.state import ScrapeState
from core.metrics import PipelineMetrics
from core.throttle import AdaptiveThrottle
from core.proxy import ProxyPool, ProxyConfig
from core.session import SessionManager
from core.worker import process_url
from core.helpers import make_context, open_page, safe_goto
from db import close_pool

log = logging.getLogger(__name__)

BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-infobars",
    "--window-size=1366,768",
    "--disable-web-security",
    "--allow-running-insecure-content",
]


class TwoStagePipeline:

    def __init__(
            self,
            provinces: list[str],
            sources: list[str],
            max_pages: int = 5,
            concurrent: int = CFG.max_concurrent,
    ):
        self.provinces = provinces
        self.sources = sources
        self.max_pages = max_pages
        self.concurrent = concurrent
        self._browser = None

        # ── Core components ───────────────────────────────────
        self.state = ScrapeState()
        self.metrics = PipelineMetrics(total_urls=0)
        self.throttle = AdaptiveThrottle(
            min_delay=CFG.delay_min,
            max_delay=20.0,
            window_size=30,
        )
        self.session_mgr = SessionManager()
        self.proxy_pool = self._init_proxy_pool()

        # ── File lưu URLs ─────────────────────────────────────
        self.hotel_urls_file = Path("hotel_urls_collected.txt")
        self.hotel_urls_file.touch(exist_ok=True)

    def _init_proxy_pool(self) -> ProxyPool:
        proxy_file = Path("proxies.txt")
        proxies = []

        if proxy_file.exists():
            for line in proxy_file.read_text("utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) >= 4:
                    try:
                        proxies.append(ProxyConfig(
                            host=parts[0],
                            port=int(parts[1]),
                            username=parts[2],
                            password=parts[3],
                            country=parts[4] if len(parts) > 4 else "VN",
                        ))
                    except (ValueError, IndexError):
                        continue

        if not proxies:
            log.warning("⚠️  Không có proxy — chạy trực tiếp")
            proxies = [ProxyConfig("", 0, "", "", "VN")]

        log.info(f"🔌  Loaded {len(proxies)} proxies")
        return ProxyPool(proxies)

    # ── Stage 1 ───────────────────────────────────────────────
    async def stage1_collect_urls(self) -> list[str]:
        from core.url_gen import URLGenerator, ListingScraper

        log.info("=" * 65)
        log.info("🔍  STAGE 1: Thu thập URL khách sạn")
        log.info("=" * 65)

        # Đọc URLs đã có
        collected: set[str] = set(
            line.strip()
            for line in self.hotel_urls_file
            .read_text("utf-8")
            .splitlines()
            if line.strip().startswith("http")
        )
        log.info(f"📋  Đã có sẵn: {len(collected):,} URLs")

        # Sinh listing URLs
        all_listing = URLGenerator.generate_all(
            provinces=self.provinces,
            sources=self.sources,
            max_pages=self.max_pages,
        )
        total_listing = sum(len(v) for v in all_listing.values())
        log.info(f"🚀  Listing URLs cần scrape: {total_listing:,}")

        sem = asyncio.Semaphore(self.concurrent)
        new_urls: list[str] = []
        lock = asyncio.Lock()

        async def _scrape_one_listing(
                listing_url: str,
                platform: str,
                pbar,
        ) -> None:
            async with sem:
                proxy = await self.proxy_pool.get()
                ctx = await make_context(self._browser, proxy)
                page = await open_page(ctx)
                try:
                    await self.throttle.wait(platform)
                    await safe_goto(page, listing_url)
                    await asyncio.sleep(random.uniform(2.0, 4.0))

                    hotel_urls = await ListingScraper.scrape_listing_page(
                        page, listing_url, platform
                    )

                    async with lock:
                        fresh = [
                            u for u in hotel_urls
                            if u not in collected
                        ]
                        if fresh:
                            collected.update(fresh)
                            new_urls.extend(fresh)
                            with self.hotel_urls_file.open(
                                    "a", encoding="utf-8"
                            ) as f:
                                f.write("\n".join(fresh) + "\n")

                        await self.throttle.record_success()
                        log.info(
                            f"  ✅ +{len(fresh):>3} URLs | "
                            f"{listing_url[:55]}"
                            )

                except Exception as e:
                    is_blocked = any(
                        kw in str(e).lower()
                        for kw in ["403", "429", "blocked", "captcha"]
                    )
                    await self.throttle.record_failure(is_blocked)
                    if is_blocked and proxy and proxy.host:
                        await self.proxy_pool.mark_bad(proxy)
                    log.warning(f"  ⚠️  Listing lỗi: {e}")
                finally:
                    await ctx.close()
                    pbar.update(1)

                    # ── Chạy tất cả listing tasks ─────────────────────────
            async with async_playwright() as pw:
                self._browser = await pw.chromium.launch(
                    headless=True,
                    args=BROWSER_ARGS,
                )
                pbar = tqdm(
                    total=total_listing,
                    desc="Stage 1 — Listing",
                    unit="page",
                    colour="cyan",
                )
                tasks = []
                for province, urls in all_listing.items():
                    for url in urls:
                        platform = urlparse(url).netloc
                        tasks.append(
                            _scrape_one_listing(url, platform, pbar)
                        )

                await asyncio.gather(*tasks, return_exceptions=True)
                pbar.close()
                await self._browser.close()

            all_hotel_urls = list(collected)
            log.info(
                f"✅  Stage 1 xong: {len(all_hotel_urls):,} hotel URLs "
                f"({len(new_urls):,} mới)"
            )
            return all_hotel_urls

    # ── Stage 2 ───────────────────────────────────────────────

    async def stage2_scrape_details(
            self,
            hotel_urls: list[str],
    ) -> None:
        log.info("=" * 65)
        log.info("🏨  STAGE 2: Scrape chi tiết khách sạn")
        log.info("=" * 65)

        pending = [
            u for u in hotel_urls
            if not self.state.should_skip(u)
        ]
        done_count = len(hotel_urls) - len(pending)

        log.info(
            f"📋  Tổng: {len(hotel_urls):,} | "
            f"Cần xử lý: {len(pending):,} | "
            f"Đã xong: {done_count:,}"
        )

        if not pending:
            log.info("✅  Tất cả URLs đã được xử lý!")
            return

        self.metrics.total_urls = len(pending)
        sem = asyncio.Semaphore(self.concurrent)
        pbar = tqdm(
            total=len(pending),
            desc="Stage 2 — Detail",
            unit="hotel",
            colour="green",
        )

        # ── Chạy theo batch để tránh memory leak ──────────────
        batch_size = CFG.browser_restart_each
        for batch_idx in range(0, len(pending), batch_size):
            batch = pending[batch_idx: batch_idx + batch_size]
            log.info(
                f"🔄  Batch {batch_idx // batch_size + 1} | "
                f"URLs {batch_idx + 1}–"
                f"{batch_idx + len(batch):,} / {len(pending):,}"
            )

            async with async_playwright() as pw:
                self._browser = await pw.chromium.launch(
                    headless=True,
                    args=BROWSER_ARGS,
                )
                try:
                    await asyncio.gather(
                        *[
                            process_url(
                                url=u,
                                browser=self._browser,
                                sem=sem,
                                state=self.state,
                                metrics=self.metrics,
                                throttle=self.throttle,
                                proxy_pool=self.proxy_pool,
                                session_mgr=self.session_mgr,
                                pbar=pbar,
                            )
                            for u in batch
                        ],
                        return_exceptions=True,
                    )
                finally:
                    await self._browser.close()

            # ── Nghỉ giữa batch ───────────────────────────────
            if batch_idx + batch_size < len(pending):
                pause = random.uniform(
                    CFG.batch_pause_min,
                    CFG.batch_pause_max,
                )
                log.info(f"⏸️  Nghỉ {pause:.0f}s giữa batch...")
                await asyncio.sleep(pause)

        pbar.close()
        log.info(self.metrics.report())

        # ── Run ───────────────────────────────────────────────────

    async def run(self) -> None:
        start = time.time()
        log.info("🚀  BẮT ĐẦU PIPELINE 2 GIAI ĐOẠN")
        log.info(f"    Tỉnh thành : {len(self.provinces)}")
        log.info(f"    Nguồn      : {', '.join(self.sources)}")
        log.info(f"    Concurrent : {self.concurrent}")
        log.info(f"    Max pages  : {self.max_pages}")

        try:
            hotel_urls = await self.stage1_collect_urls()
            await self.stage2_scrape_details(hotel_urls)
        except KeyboardInterrupt:
            log.info("🛑  Dừng bởi người dùng.")
        except Exception as e:
            log.error(f"❌  Pipeline lỗi: {e}", exc_info=True)
        finally:
            elapsed = time.time() - start
            log.info(
                f"⏱️  Tổng thời gian: {elapsed / 3600:.1f} giờ"
            )
            log.info(self.metrics.report())
            close_pool()
