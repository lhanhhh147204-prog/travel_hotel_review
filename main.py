# main.py
from __future__ import annotations

import asyncio
import logging
import sys

from config          import CFG
from core.pipeline   import TwoStagePipeline
from analytics.analyzer  import DataAnalyzer
from analytics.estimator import TimeEstimator
from db              import close_pool

log = logging.getLogger(__name__)

# ── Province tiers ────────────────────────────────────────────
# main.py (tiếp)
PROVINCE_TIERS: dict[str, dict] = {
    "tier_1": {
        "provinces": [
            "Hà Nội",         "Hồ Chí Minh",
            "Đà Nẵng",        "Khánh Hòa",
            "Kiên Giang",     "Lâm Đồng",
            "Quảng Nam",      "Thừa Thiên Huế",
            "Quảng Ninh",     "Lào Cai",
        ],
        "target_reviews": 50_000,
        "target_hotels":  500,
        "sources": [
            "booking", "agoda", "tripadvisor",
            "airbnb", "ivivu", "google_maps",
            "traveloka",
        ],
    },
    "tier_2": {
        "provinces": [
            "Bình Thuận",     "Bà Rịa-Vũng Tàu",
            "Ninh Bình",      "Hải Phòng",
            "Bình Định",      "Phú Yên",
            "Đắk Lắk",       "Hà Giang",
            "Điện Biên",      "Sơn La",
            "Hòa Bình",       "Thanh Hóa",
            "Nghệ An",        "Quảng Bình",
        ],
        "target_reviews": 20_000,
        "target_hotels":  200,
        "sources": [
            "booking", "agoda", "ivivu",
            "mytour", "google_maps", "traveloka",
        ],
    },
    "tier_3": {
        "provinces": [
            "An Giang",       "Bạc Liêu",
            "Bắc Giang",      "Bắc Kạn",
            "Bắc Ninh",       "Bến Tre",
            "Bình Dương",     "Bình Phước",
            "Cà Mau",         "Cần Thơ",
            "Cao Bằng",       "Đắk Nông",
            "Đồng Nai",       "Đồng Tháp",
            "Gia Lai",        "Hà Nam",
            "Hà Tĩnh",        "Hải Dương",
            "Hậu Giang",      "Hưng Yên",
            "Kon Tum",        "Lai Châu",
            "Lạng Sơn",       "Long An",
            "Nam Định",       "Ninh Thuận",
            "Phú Thọ",        "Quảng Ngãi",
            "Quảng Trị",      "Sóc Trăng",
            "Tây Ninh",       "Thái Bình",
            "Thái Nguyên",    "Tiền Giang",
            "Trà Vinh",       "Tuyên Quang",
            "Vĩnh Long",      "Vĩnh Phúc",
            "Yên Bái",
        ],
        "target_reviews": 5_000,
        "target_hotels":  50,
        "sources": [
            "booking", "ivivu",
            "google_maps", "mytour",
        ],
    },
}


def get_all_provinces() -> list[str]:
    """Lấy danh sách tất cả tỉnh thành."""
    provinces = []
    for tier_cfg in PROVINCE_TIERS.values():
        provinces.extend(tier_cfg["provinces"])
    return list(dict.fromkeys(provinces))  # giữ thứ tự, bỏ duplicate


def get_all_sources() -> list[str]:
    """Lấy danh sách tất cả nguồn dữ liệu."""
    sources: set[str] = set()
    for tier_cfg in PROVINCE_TIERS.values():
        sources.update(tier_cfg["sources"])
    return sorted(sources)


def calculate_targets() -> dict:
    """Tính tổng mục tiêu scrape."""
    total_reviews = 0
    total_hotels  = 0
    breakdown     = {}

    for tier, cfg in PROVINCE_TIERS.items():
        n       = len(cfg["provinces"])
        reviews = n * cfg["target_reviews"]
        hotels  = n * cfg["target_hotels"]
        total_reviews += reviews
        total_hotels  += hotels
        breakdown[tier] = {
            "provinces": n,
            "reviews":   reviews,
            "hotels":    hotels,
        }

    return {
        "total_reviews": total_reviews,
        "total_hotels":  total_hotels,
        "breakdown":     breakdown,
    }


def print_banner() -> None:
    """In banner khởi động."""
    print("\n" + "═" * 65)
    print("  🏨  HOTEL REVIEW SCRAPER — VIETNAM")
    print("  📊  ETL Pipeline v3.0")
    print("═" * 65)

    targets = calculate_targets()
    print(f"  Tổng tỉnh thành  : {len(get_all_provinces()):>6}")
    print(f"  Tổng khách sạn   : {targets['total_hotels']:>6,}")
    print(f"  Tổng đánh giá    : {targets['total_reviews']:>6,}")
    print(f"  Nguồn dữ liệu    : {', '.join(get_all_sources())}")
    print("═" * 65 + "\n")

    for tier, info in targets["breakdown"].items():
        print(
            f"  {tier.upper():<8}: "
            f"{info['provinces']:>3} tỉnh | "
            f"{info['hotels']:>6,} KS | "
            f"{info['reviews']:>8,} reviews"
        )
    print("═" * 65 + "\n")


# ── Mode handlers ─────────────────────────────────────────────

async def run_scrape(
        tier:       str  = "all",
        max_pages:  int  = 5,
        concurrent: int  = CFG.max_concurrent,
) -> None:
    """Chạy pipeline scraping."""
    if tier == "all":
        provinces = get_all_provinces()
        sources   = get_all_sources()
    elif tier in PROVINCE_TIERS:
        provinces = PROVINCE_TIERS[tier]["provinces"]
        sources   = PROVINCE_TIERS[tier]["sources"]
    else:
        log.error(f"Tier không hợp lệ: {tier}")
        return

    log.info(
        f"🚀  Bắt đầu scrape tier={tier} | "
        f"{len(provinces)} tỉnh | "
        f"concurrent={concurrent}"
    )

    pipeline = TwoStagePipeline(
        provinces  = provinces,
        sources    = sources,
        max_pages  = max_pages,
        concurrent = concurrent,
    )
    await pipeline.run()


def run_analyze() -> None:
    """Chạy phân tích và xuất báo cáo."""
    log.info("📊  Bắt đầu phân tích dữ liệu...")
    analyzer = DataAnalyzer(conn_str=CFG.db_conn_str)
    analyzer.run_all()


def run_estimate(
        concurrent: int = CFG.max_concurrent,
) -> None:
    """In ước tính thời gian chạy."""
    targets = calculate_targets()
    TimeEstimator.print_estimate(
        total_hotels=targets["total_hotels"],
        concurrent=concurrent,
    )


def run_stage1_only(
        tier:      str = "tier_1",
        max_pages: int = 5,
) -> None:
    """Chỉ chạy Stage 1 — thu thập URLs."""
    async def _run():
        provinces = PROVINCE_TIERS.get(tier, {}).get(
            "provinces", get_all_provinces()
        )
        sources   = PROVINCE_TIERS.get(tier, {}).get(
            "sources", get_all_sources()
        )
        pipeline  = TwoStagePipeline(
            provinces  = provinces,
            sources    = sources,
            max_pages  = max_pages,
        )
        urls = await pipeline.stage1_collect_urls()
        return urls  # ← THÊM DÒNG NÀY

    urls = asyncio.run(_run())  # ← NHẬN RETURN VALUE
    log.info(f"✅  Stage 1 xong: {len(urls):,} URLs")
    asyncio.run(_run())


def run_stage2_only(
        url_file:   str = "hotel_urls_collected.txt",
        concurrent: int = CFG.max_concurrent,
) -> None:
    """Chỉ chạy Stage 2 — scrape chi tiết từ file URLs."""
    from pathlib import Path

    path = Path(url_file)
    if not path.exists():
        log.error(f"File không tồn tại: {url_file}")
        return

    urls = [
        line.strip()
        for line in path.read_text("utf-8").splitlines()
        if line.strip().startswith("http")
    ]
    log.info(f"📋  Loaded {len(urls):,} URLs từ {url_file}")

    async def _run():
        pipeline = TwoStagePipeline(
            provinces  = [],
            sources    = [],
            concurrent = concurrent,
        )
        await pipeline.stage2_scrape_details(urls)

    asyncio.run(_run())


# ── CLI ───────────────────────────────────────────────────────

def print_help() -> None:
    print("""
╔══════════════════════════════════════════════════════════════╗
║           HOTEL REVIEW SCRAPER — HƯỚNG DẪN SỬ DỤNG           ║
╠══════════════════════════════════════════════════════════════╣
║  python main.py scrape [tier] [max_pages] [concurrent]       ║
║      tier      : all | tier_1 | tier_2 | tier_3              ║ 
║      max_pages : số trang listing (mặc định: 5)              ║
║      concurrent: số luồng song song (mặc định: 3)            ║
║                                                              ║
║  python main.py analyze                                      ║
║      Xuất báo cáo Excel từ dữ liệu đã scrape                 ║
║                                                              ║
║  python main.py estimate [concurrent]                        ║
║      Ước tính thời gian chạy                                 ║
║                                                              ║
║  python main.py stage1 [tier] [max_pages]                    ║
║      Chỉ chạy Stage 1 — thu thập URLs                        ║
║                                                              ║
║  python main.py stage2 [url_file] [concurrent]               ║
║      Chỉ chạy Stage 2 — scrape chi tiết                      ║
║                                                              ║
║  python main.py help                                         ║
║      Hiển thị hướng dẫn này                                  ║
╚══════════════════════════════════════════════════════════════╝
    """)


def main() -> None:
    print_banner()
    args = sys.argv[1:]

    if not args or args[0] in ("help", "--help", "-h"):
        print_help()
        return

    mode = args[0].lower()

    # ── scrape ────────────────────────────────────────────────
    if mode == "scrape":
        tier       = args[1] if len(args) > 1 else "all"
        max_pages  = int(args[2]) if len(args) > 2 else 5
        concurrent = int(args[3]) if len(args) > 3 else CFG.max_concurrent
        try:
            asyncio.run(
                run_scrape(
                    tier       = tier,
                    max_pages  = max_pages,
                    concurrent = concurrent,
                )
            )
        except KeyboardInterrupt:
            log.info("🛑  Dừng bởi người dùng.")
        finally:
            close_pool()

    # ── analyze ───────────────────────────────────────────────
    elif mode == "analyze":
        try:
            run_analyze()
        except Exception as e:
            log.error(f"❌  Analyze lỗi: {e}", exc_info=True)

    # ── estimate ──────────────────────────────────────────────
    elif mode == "estimate":
        concurrent = int(args[1]) if len(args) > 1 else CFG.max_concurrent
        run_estimate(concurrent=concurrent)

    # ── stage1 ────────────────────────────────────────────────
    elif mode == "stage1":
        tier      = args[1] if len(args) > 1 else "tier_1"
        max_pages = int(args[2]) if len(args) > 2 else 5
        try:
            run_stage1_only(tier=tier, max_pages=max_pages)
        except KeyboardInterrupt:
            log.info("🛑  Dừng bởi người dùng.")
        finally:
            close_pool()

    # ── stage2 ────────────────────────────────────────────────
    elif mode == "stage2":
        url_file   = args[1] if len(args) > 1 else "hotel_urls_collected.txt"
        concurrent = int(args[2]) if len(args) > 2 else CFG.max_concurrent
        try:
            run_stage2_only(
                url_file   = url_file,
                concurrent = concurrent,
            )
        except KeyboardInterrupt:
            log.info("🛑  Dừng bởi người dùng.")
        finally:
            close_pool()

    else:
        log.error(f"❌  Lệnh không hợp lệ: {mode}")
        print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()