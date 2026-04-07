"""
example_run.py
==============
Ví dụ crawl 1 triệu record từ một website mẫu.
Chạy:  python example_run.py
"""

import asyncio
import logging
from typing import Optional

from core.proxy import ProxyPool
from core.crawler import Crawler, CrawlerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# 1. Tạo danh sách URL cần crawl (1 triệu URL)
# ---------------------------------------------------------------------------

def generate_urls(base: str, total: int = 1_000_000) -> list[str]:
    """Sinh URL theo page hoặc ID – tuỳ chỉnh theo target."""
    return [f"{base}?page={i}" for i in range(1, total + 1)]


# ---------------------------------------------------------------------------
# 2. Parser: nhận HTML → trả về dict record (tuỳ chỉnh)
# ---------------------------------------------------------------------------

async def parse(html: str, url: str) -> Optional[dict]:
    """
    Ví dụ parse đơn giản.
    Thực tế: dùng BeautifulSoup / lxml / parsel / re để extract.
    """
    if not html:
        return None
    # --- demo: chỉ lưu url + độ dài html ---
    return {
        "url":     url,
        "length":  len(html),
        # "title": BeautifulSoup(html).find("title").text,
        # "data":  ...
    }


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------

async def main() -> None:
    # Bước 1: xây proxy pool từ free sources (tự động tải + validate)
    print("⏳ Đang tải và validate proxy miễn phí...")
    pool = await ProxyPool.build(
        cache_file="proxy_cache.json",   # lưu cache để lần sau nhanh hơn
        concurrent=100,                  # validate 100 proxy cùng lúc
        timeout=8,
    )
    print(f"✅ {pool}")

    if len(pool) == 0:
        print("❌ Không có proxy nào hoạt động. Vui lòng kiểm tra kết nối.")
        return

    # Bước 2: cấu hình crawler
    cfg = CrawlerConfig(
        concurrency=200,         # 200 worker song song
        rps=100.0,               # tối đa 100 req/giây
        timeout=20.0,
        max_retries=3,
        checkpoint_file="checkpoint.json",
        output_file="output.jsonl",   # mỗi dòng = 1 JSON record
    )

    # Bước 3: sinh URL và chạy
    urls    = generate_urls("https://example-target.com/items", total=1_000_000)
    crawler = Crawler(pool, cfg)

    print(f"🚀 Bắt đầu crawl {len(urls):,} URL...")
    await crawler.run(urls, parse)
    print("🎉 Hoàn tất!")


if __name__ == "__main__":
    asyncio.run(main())
