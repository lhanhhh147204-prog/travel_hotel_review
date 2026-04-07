"""
core/proxy.py
=============
Proxy pool thống nhất, hỗ trợ:
  - Free proxy tự động từ nhiều nguồn công khai
  - Async validation song song (asyncio + aiohttp)
  - Smart rotation: weighted-random theo success-rate
  - Circuit-breaker: tạm khoá proxy lỗi, tự hồi phục sau cooldown
  - Rate-limiter per-proxy để tránh bị block
  - Metrics chi tiết (latency, success-rate, request count)
  - Export/Import proxy list (JSON)
  - Drop-in compatible với cả sync (requests) và async (aiohttp)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import aiohttp
import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hằng số
# ---------------------------------------------------------------------------

DEFAULT_TEST_URL      = "https://httpbin.org/ip"
DEFAULT_TIMEOUT       = 10          # giây
DEFAULT_COOLDOWN      = 300         # giây khoá proxy xấu (5 phút)
DEFAULT_MAX_FAILS     = 3           # lỗi liên tiếp trước khi khoá
MIN_VALID_POOL        = 10          # số proxy tối thiểu trước khi crawl
DEFAULT_RATE_LIMIT    = 2           # req/giây trên mỗi proxy
DEFAULT_CONCURRENT    = 50          # goroutine validate cùng lúc

# ---------------------------------------------------------------------------
# Nguồn free proxy công khai (không cần trả tiền)
# ---------------------------------------------------------------------------

FREE_PROXY_SOURCES: list[dict] = [
    {
        "url":    "https://www.proxy-list.download/api/v1/get?type=https",
        "parser": "plain_text",   # mỗi dòng là 1 proxy ip:port
    },
    {
        "url":    "https://www.proxy-list.download/api/v1/get?type=http",
        "parser": "plain_text",
    },
    {
        "url":    "https://api.proxyscrape.com/v2/?request=displayproxies"
                  "&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
        "parser": "plain_text",
    },
    {
        "url":    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "parser": "plain_text",
    },
    {
        "url":    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "parser": "plain_text",
    },
    {
        "url":    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
        "parser": "plain_text",
    },
]


# ---------------------------------------------------------------------------
# Trạng thái proxy
# ---------------------------------------------------------------------------

class ProxyState(Enum):
    HEALTHY  = auto()   # đang hoạt động tốt
    COOLING  = auto()   # tạm khoá, đang đếm ngược cooldown
    DEAD     = auto()   # đã thử hồi phục, vẫn lỗi → loại hẳn


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ProxyConfig:
    host:     str
    port:     int
    username: str = ""
    password: str = ""
    country:  str = "unknown"

    # ── runtime metrics ──────────────────────────────────────────────────
    success:      int   = field(default=0, repr=False)
    fail:         int   = field(default=0, repr=False)
    consec_fail:  int   = field(default=0, repr=False)
    avg_latency:  float = field(default=0.0, repr=False)
    state:        ProxyState = field(default=ProxyState.HEALTHY, repr=False)
    locked_until: float = field(default=0.0, repr=False)
    last_used:    float = field(default=0.0, repr=False)

    @property
    def url(self) -> str:
        if self.username:
            return (f"http://{self.username}:{self.password}"
                    f"@{self.host}:{self.port}")
        return f"http://{self.host}:{self.port}"

    @property
    def success_rate(self) -> float:
        total = self.success + self.fail
        return self.success / total if total else 0.0

    @property
    def weight(self) -> float:
        """Trọng số cho weighted-random: ưu tiên proxy nhanh + ổn định."""
        if self.state != ProxyState.HEALTHY:
            return 0.0
        sr   = self.success_rate
        lat  = self.avg_latency or DEFAULT_TIMEOUT
        # công thức: success_rate / latency; tối thiểu 0.01 tránh chia 0
        return max(sr / lat, 0.01)

    def record_success(self, latency: float) -> None:
        self.success     += 1
        self.consec_fail  = 0
        self.state        = ProxyState.HEALTHY
        # exponential moving average cho latency
        alpha = 0.3
        self.avg_latency  = (alpha * latency
                             + (1 - alpha) * (self.avg_latency or latency))

    def record_fail(self, cooldown: float = DEFAULT_COOLDOWN,
                    max_fails: int = DEFAULT_MAX_FAILS) -> None:
        self.fail        += 1
        self.consec_fail += 1
        if self.consec_fail >= max_fails:
            self.state        = ProxyState.COOLING
            self.locked_until = time.time() + cooldown
            log.debug("Proxy %s:%d → COOLING (%.0fs)",
                      self.host, self.port, cooldown)

    def try_recover(self) -> bool:
        """Gọi khi cooldown hết hạn; trả về True nếu được thử lại."""
        if self.state == ProxyState.COOLING and time.time() > self.locked_until:
            self.state       = ProxyState.HEALTHY
            self.consec_fail = 0
            return True
        return False


# ---------------------------------------------------------------------------
# Parser helper
# ---------------------------------------------------------------------------

def _parse_plain_text(text: str) -> list[ProxyConfig]:
    configs = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) == 2:
            try:
                configs.append(ProxyConfig(host=parts[0], port=int(parts[1])))
            except ValueError:
                pass
    return configs


PARSERS = {
    "plain_text": _parse_plain_text,
}


# ---------------------------------------------------------------------------
# Async validator
# ---------------------------------------------------------------------------

async def _validate_one(
    proxy:    ProxyConfig,
    test_url: str = DEFAULT_TEST_URL,
    timeout:  float = DEFAULT_TIMEOUT,
) -> bool:
    """Kiểm tra 1 proxy; cập nhật metrics trực tiếp."""
    t0 = time.monotonic()
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                test_url,
                proxy=proxy.url,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    proxy.record_success(time.monotonic() - t0)
                    return True
    except Exception:
        pass
    proxy.record_fail()
    return False


async def validate_proxies_async(
    proxies:    list[ProxyConfig],
    test_url:   str   = DEFAULT_TEST_URL,
    timeout:    float = DEFAULT_TIMEOUT,
    concurrent: int   = DEFAULT_CONCURRENT,
) -> list[ProxyConfig]:
    """Validate toàn bộ list song song; trả về list proxy sống."""
    sem = asyncio.Semaphore(concurrent)

    async def _bounded(p: ProxyConfig) -> Optional[ProxyConfig]:
        async with sem:
            ok = await _validate_one(p, test_url, timeout)
            return p if ok else None

    results = await asyncio.gather(*(_bounded(p) for p in proxies))
    alive   = [r for r in results if r is not None]
    log.info("Validate xong: %d/%d proxy sống", len(alive), len(proxies))
    return alive


# ---------------------------------------------------------------------------
# ProxyPool chính
# ---------------------------------------------------------------------------

class ProxyPool:
    """
    Pool proxy thống nhất.

    Sử dụng:
        pool = await ProxyPool.build()          # tự tải + validate free proxy
        proxy = pool.get()                      # lấy proxy tốt nhất (sync-safe)
        proxy = await pool.aget()               # async version
        pool.report_success(proxy, latency)
        pool.report_fail(proxy)
    """

    def __init__(
        self,
        proxies:   list[ProxyConfig] | None = None,
        cooldown:  float = DEFAULT_COOLDOWN,
        max_fails: int   = DEFAULT_MAX_FAILS,
        rate_limit: float = DEFAULT_RATE_LIMIT,
    ):
        self._pool:       list[ProxyConfig] = proxies or []
        self._cooldown    = cooldown
        self._max_fails   = max_fails
        self._rate_limit  = rate_limit          # req/s per proxy
        self._lock        = asyncio.Lock()
        self._rate_cache: dict[str, list[float]] = {}   # url → timestamps

    # ── factory ────────────────────────────────────────────────────────────

    @classmethod
    async def build(
        cls,
        sources:   list[dict]  | None = None,
        cache_file: str | None = None,
        test_url:  str   = DEFAULT_TEST_URL,
        timeout:   float = DEFAULT_TIMEOUT,
        concurrent: int  = DEFAULT_CONCURRENT,
        **kwargs,
    ) -> "ProxyPool":
        """
        Xây pool từ đầu:
          1. Đọc cache nếu có
          2. Tải từ nguồn free proxy
          3. Validate song song
          4. Lưu cache
        """
        instance = cls(**kwargs)

        raw: list[ProxyConfig] = []

        # 1. Cache
        if cache_file and Path(cache_file).exists():
            raw = instance.load(cache_file)
            log.info("Đọc %d proxy từ cache %s", len(raw), cache_file)

        # 2. Free sources
        srcs = sources or FREE_PROXY_SOURCES
        fetched = await instance._fetch_all(srcs)
        raw.extend(fetched)

        # dedup theo url
        seen:    set[str]         = set()
        unique:  list[ProxyConfig] = []
        for p in raw:
            if p.url not in seen:
                seen.add(p.url)
                unique.append(p)

        log.info("Tổng proxy thô (dedup): %d", len(unique))

        # 3. Validate
        alive = await validate_proxies_async(unique, test_url, timeout, concurrent)
        instance._pool = alive

        # 4. Lưu cache
        if cache_file:
            instance.save(cache_file)

        return instance

    # ── fetch free proxy ────────────────────────────────────────────────────

    async def _fetch_all(self, sources: list[dict]) -> list[ProxyConfig]:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(None, self._fetch_one, src)
                 for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_proxies: list[ProxyConfig] = []
        for r in results:
            if isinstance(r, list):
                all_proxies.extend(r)
        return all_proxies

    @staticmethod
    def _fetch_one(source: dict) -> list[ProxyConfig]:
        url    = source["url"]
        parser = PARSERS.get(source.get("parser", "plain_text"), _parse_plain_text)
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            result = parser(resp.text)
            log.info("Tải %d proxy từ %s", len(result), url)
            return result
        except Exception as exc:
            log.warning("Không tải được %s: %s", url, exc)
            return []

    # ── get proxy ───────────────────────────────────────────────────────────

    def get(self) -> Optional[ProxyConfig]:
        """Lấy proxy tốt nhất (weighted-random). Thread-safe với GIL."""
        self._try_recover_cooling()

        candidates = [p for p in self._pool if p.state == ProxyState.HEALTHY]
        if not candidates:
            log.warning("Không còn proxy healthy! Pool size=%d", len(self._pool))
            return None

        weights = [p.weight for p in candidates]
        chosen  = random.choices(candidates, weights=weights, k=1)[0]

        # rate-limit: chờ nếu proxy này được dùng quá nhanh
        self._apply_rate_limit(chosen)

        chosen.last_used = time.time()
        return chosen

    async def aget(self) -> Optional[ProxyConfig]:
        """Async version của get()."""
        async with self._lock:
            return self.get()

    # ── report kết quả ─────────────────────────────────────────────────────

    def report_success(self, proxy: ProxyConfig, latency: float = 1.0) -> None:
        proxy.record_success(latency)

    def report_fail(self, proxy: ProxyConfig) -> None:
        proxy.record_fail(self._cooldown, self._max_fails)
        if proxy.state == ProxyState.DEAD:
            self._pool = [p for p in self._pool if p is not proxy]
            log.warning("Loại hẳn proxy %s:%d (DEAD)", proxy.host, proxy.port)

    # ── refresh (refill khi pool cạn) ──────────────────────────────────────

    async def refresh(
        self,
        sources:   list[dict] | None = None,
        test_url:  str   = DEFAULT_TEST_URL,
        timeout:   float = DEFAULT_TIMEOUT,
        concurrent: int  = DEFAULT_CONCURRENT,
    ) -> int:
        """Tải thêm proxy mới và validate; trả về số lượng thêm vào."""
        srcs    = sources or FREE_PROXY_SOURCES
        fetched = await self._fetch_all(srcs)

        existing_urls = {p.url for p in self._pool}
        new_raw = [p for p in fetched if p.url not in existing_urls]

        alive = await validate_proxies_async(new_raw, test_url, timeout, concurrent)
        self._pool.extend(alive)
        log.info("Refresh: thêm %d proxy mới, pool = %d", len(alive), len(self._pool))
        return len(alive)

    # ── helpers ─────────────────────────────────────────────────────────────

    def _try_recover_cooling(self) -> None:
        for p in self._pool:
            if p.state == ProxyState.COOLING:
                p.try_recover()

    def _apply_rate_limit(self, proxy: ProxyConfig) -> None:
        key  = proxy.url
        now  = time.monotonic()
        hist = self._rate_cache.setdefault(key, [])
        # giữ lại timestamps trong 1 giây
        hist[:] = [t for t in hist if now - t < 1.0]
        if len(hist) >= self._rate_limit:
            sleep_t = 1.0 - (now - hist[0])
            if sleep_t > 0:
                time.sleep(sleep_t)
        hist.append(time.monotonic())

    # ── persist ─────────────────────────────────────────────────────────────

    def save(self, path: str) -> None:
        data = [{"host": p.host, "port": p.port,
                 "username": p.username, "password": p.password,
                 "country": p.country}
                for p in self._pool]
        Path(path).write_text(json.dumps(data, indent=2), encoding="utf-8")
        log.info("Lưu %d proxy → %s", len(data), path)

    @staticmethod
    def load(path: str) -> list[ProxyConfig]:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return [ProxyConfig(**d) for d in data]

    # ── context manager ─────────────────────────────────────────────────────

    async def __aenter__(self) -> "ProxyPool":
        return self

    async def __aexit__(self, *_) -> None:
        pass   # cleanup nếu cần

    # ── stats ───────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        total   = len(self._pool)
        healthy = sum(1 for p in self._pool if p.state == ProxyState.HEALTHY)
        cooling = sum(1 for p in self._pool if p.state == ProxyState.COOLING)
        dead    = sum(1 for p in self._pool if p.state == ProxyState.DEAD)
        rates   = [p.success_rate for p in self._pool if p.success + p.fail > 0]
        lats    = [p.avg_latency  for p in self._pool if p.avg_latency > 0]
        return {
            "total":        total,
            "healthy":      healthy,
            "cooling":      cooling,
            "dead":         dead,
            "avg_success_rate": round(sum(rates) / len(rates), 3) if rates else 0,
            "avg_latency_s":    round(sum(lats)  / len(lats),  3) if lats  else 0,
        }

    def __len__(self) -> int:
        return len(self._pool)

    def __repr__(self) -> str:
        s = self.stats
        return (f"<ProxyPool total={s['total']} healthy={s['healthy']} "
                f"cooling={s['cooling']} dead={s['dead']}>")
