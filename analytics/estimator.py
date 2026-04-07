# analytics/estimator.py
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class TimeEstimator:
    """Ước tính thời gian chạy pipeline."""

    @staticmethod
    def estimate(
            total_hotels:      int   = 35_280,
            avg_time_per_ks:   float = 45.0,
            concurrent:        int   = 3,
            proxy_delay:       float = 5.0,
    ) -> dict:
        effective_time = (avg_time_per_ks + proxy_delay) / concurrent
        total_seconds  = total_hotels * effective_time
        total_hours    = total_seconds / 3600
        total_days     = total_hours   / 24

        return {
            "total_hotels":  total_hotels,
            "total_seconds": round(total_seconds, 0),
            "total_hours":   round(total_hours,   1),
            "total_days":    round(total_days,     1),
            "breakdown": {
                "stage1_listing": f"{total_hours * 0.2:.1f} giờ",
                "stage2_detail":  f"{total_hours * 0.8:.1f} giờ",
            },
            "recommendation": (
                f"Chạy 24/7 trên VPS với {concurrent} concurrent "
                f"→ hoàn tất trong {total_days:.0f} ngày"
            ),
        }

    @staticmethod
    def print_estimate(
            total_hotels: int   = 35_280,
            concurrent:   int   = 3,
    ) -> None:
        est = TimeEstimator.estimate(
            total_hotels=total_hotels,
            concurrent=concurrent,
        )
        print("\n" + "═" * 55)
        print("⏱️   ƯỚC TÍNH THỜI GIAN CHẠY")
        print("═" * 55)
        print(f"  Tổng khách sạn   : {est['total_hotels']:>10,}")
        print(f"  Tổng thời gian   : {est['total_hours']:>10.1f} giờ")
        print(f"  Số ngày          : {est['total_days']:>10.1f} ngày")
        print(f"  Stage 1 listing  : {est['breakdown']['stage1_listing']:>12}")
        print(f"  Stage 2 detail   : {est['breakdown']['stage2_detail']:>12}")
        print(f"  Khuyến nghị      : {est['recommendation']}")
        print("═" * 55 + "\n")