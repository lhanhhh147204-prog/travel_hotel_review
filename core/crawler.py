"""
core/crawler.py
===============
Crawler bất đồng bộ 1 triệu record, tích hợp ProxyPool.

Tính năng:
  - Async với aiohttp + asyncio.Queue
  - Tự động rotate proxy, retry thông minh
  - Checkpoint (lưu progress) để resume khi bị ngắt
  - Rate-limit toàn cục (req/s) + per-domain
  - Dedup URL (bloom-filter nhẹ qua set)
  - Streaming JSONL output (không cần load toàn bộ vào RAM)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Coroutine

import aiohttp

from core.proxy import ProxyConfig, ProxyPool

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

dataclass_like_class = None   # placeholder – xem class CrawlerConfig bên dưới


class CrawlerConfig:
    def __init__(
        self,
        concurrency:     int   = 200,     # số worker đồng thời
        rps:             float = 50.0,    # request/giây toàn cục
        timeout:         float = 20.0,    # giây mỗi request
        max_retries:     int   = 3,
        retry_delay:     float = 2.0,
        checkpoint_file: str   = "checkpoint.json",
        output_file:     str   = "output.jsonl",
        headers:         dict  | None = None,
    ):
        self.concurrency     = concurrency
        self.rps             = rps
        self.timeout         = timeout
        self.max_retries     = max_retries
        self.retry_delay     = retry_delay
        self.checkpoint_file = checkpoint_file
        self.output_file     = output_file
        self.headers         = headers or {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

class Crawler:
    """
    Sử dụng:

        pool = await ProxyPool.build()
        cfg  = CrawlerConfig(concurrency=200, rps=50)
        crawler = Crawler(pool, cfg)

        async def parse(html: str, url: str) -> dict:
            ...  # parse dữ liệu từ HTML, trả về dict

        await crawler.run(urls, parse)
    """

    def __init__(self, pool: ProxyPool, cfg: CrawlerConfig | None = None):
        self._pool    = pool
        self._cfg     = cfg or CrawlerConfig()
        self._done:   set[str] = set()    # dedup
        self._out_fh  = None
        self._lock    = asyncio.Lock()
        self._stats   = {"ok": 0, "fail": 0, "skip": 0}
        self._token_ts: list[float] = []  # token-bucket cho RPS

    # ── public API ──────────────────────────────────────────────────────────

    async def run(
        self,
        urls:   list[str],
        parser: Callable[[str, str], Coroutine[Any, Any, dict | None]],
    ) -> None:
        """Crawl toàn bộ urls; gọi parser(html, url) cho mỗi trang."""
        self._load_checkpoint()
        remaining = [u for u in urls if u not in self._done]
        log.info("Còn %d URL cần crawl (đã làm %d)", len(remaining), len(self._done))

        queue: asyncio.Queue[str] = asyncio.Queue()
        for u in remaining:
            queue.put_nowait(u)

        cfg = self._cfg
        async with asyncio.timeout(None):
            self._out_fh = open(cfg.output_file, "a", encoding="utf-8")
            try:
                workers = [
                    asyncio.create_task(self._worker(queue, parser))
                    for _ in range(cfg.concurrency)
                ]
                await queue.join()
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
            finally:
                self._out_fh.close()
                self._save_checkpoint()

        log.info("Hoàn tất. Stats: %s | Proxy: %s", self._stats, self._pool.stats)

    # ── worker ──────────────────────────────────────────────────────────────

    async def _worker(
        self,
        queue:  asyncio.Queue[str],
        parser: Callable,
    ) -> None:
        while True:
            url = await queue.get()
            try:
                await self._process(url, parser)
            except asyncio.CancelledError:
                queue.task_done()
                raise
            except Exception as exc:
                log.error("Worker lỗi không mong đợi [%s]: %s", url, exc)
            finally:
                queue.task_done()

    async def _process(self, url: str, parser: Callable) -> None:
        cfg = self._cfg
        for attempt in range(cfg.max_retries):
            proxy = await self._pool.aget()
            if proxy is None:
                log.warning("Hết proxy! Đang refresh...")
                await self._pool.refresh()
                await asyncio.sleep(5)
                continue

            await self._throttle()

            t0   = time.monotonic()
            html = await self._fetch(url, proxy)
            lat  = time.monotonic() - t0

            if html is None:
                self._pool.report_fail(proxy)
                self._stats["fail"] += 1
                if attempt < cfg.max_retries - 1:
                    await asyncio.sleep(cfg.retry_delay * (attempt + 1))
                continue

            # thành công
            self._pool.report_success(proxy, lat)
            data = await parser(html, url)
            if data:
                await self._write(data)
            async with self._lock:
                self._done.add(url)
                self._stats["ok"] += 1

                # checkpoint mỗi 10 000 record
                if len(self._done) % 10_000 == 0:
                    self._save_checkpoint()
                    log.info("Progress: %d done | proxy: %s",
                             len(self._done), self._pool.stats)
            return

        self._stats["fail"] += 1

    # ── fetch ────────────────────────────────────────────────────────────────

    async def _fetch(self, url: str, proxy: ProxyConfig) -> str | None:
        cfg = self._cfg
        try:
            connector = aiohttp.TCPConnector(ssl=False, limit=0)
            async with aiohttp.ClientSession(
                connector=connector,
                headers=cfg.headers,
            ) as session:
                async with session.get(
                    url,
                    proxy=proxy.url,
                    timeout=aiohttp.ClientTimeout(total=cfg.timeout),
                    allow_redirects=True,
                ) as resp:
                    if resp.status in (403, 429, 503):
                        # bị block → đánh dấu proxy xấu
                        self._pool.report_fail(proxy)
                        return None
                    if resp.status != 200:
                        return None
                    return await resp.text(errors="replace")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None
        except Exception as exc:
            log.debug("Fetch lỗi [%s]: %s", url, exc)
            return None

    # ── throttle (token-bucket) ─────────────────────────────────────────────

    async def _throttle(self) -> None:
        rps = self._cfg.rps
        now = time.monotonic()
        self._token_ts = [t for t in self._token_ts if now - t < 1.0]
        if len(self._token_ts) >= rps:
            wait = 1.0 - (now - self._token_ts[0])
            if wait > 0:
                await asyncio.sleep(wait)
        self._token_ts.append(time.monotonic())

    # ── output ───────────────────────────────────────────────────────────────

    async def _write(self, data: dict) -> None:
        async with self._lock:
            self._out_fh.write(json.dumps(data, ensure_ascii=False) + "\n")

    # ── checkpoint ───────────────────────────────────────────────────────────

    def _save_checkpoint(self) -> None:
        path = Path(self._cfg.checkpoint_file)
        path.write_text(
            json.dumps({"done": list(self._done)}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _load_checkpoint(self) -> None:
        path = Path(self._cfg.checkpoint_file)
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            self._done = set(data.get("done", []))
            log.info("Resume: %d URL đã crawl trước đó", len(self._done))
