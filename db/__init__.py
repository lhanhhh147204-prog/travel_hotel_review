# db/__init__.py
from __future__ import annotations

import logging
from config          import CFG
from db.repository   import ConnectionPool, HotelRepository

log = logging.getLogger(__name__)

# ── Khởi tạo pool & repository ────────────────────────────────
_pool = ConnectionPool(
    conn_str  = CFG.db_conn_str,
    pool_size = CFG.max_concurrent + 2,
)
_repo = HotelRepository(_pool)


def save_to_db(
        hotel:   dict,
        rooms:   list[dict],
        reviews: list[dict],
) -> dict[str, int]:
    """
    Hàm wrapper — gọi từ asyncio.to_thread().
    Returns: stats dict với rooms_saved, reviews_saved, reviews_skip.
    """
    try:
        stats = _repo.save_hotel(hotel, rooms, reviews)
        log.debug(
            f"DB saved: {hotel['name'][:30]} | "
            f"rooms={stats['rooms_saved']} | "
            f"reviews={stats['reviews_saved']} | "
            f"skip={stats['reviews_skip']}"
        )
        return stats
    except Exception as e:
        log.error(
            f"save_to_db error [{hotel['url'][:60]}]: {e}"
        )
        raise


def get_repo() -> HotelRepository:
    return _repo


def get_pool() -> ConnectionPool:
    return _pool


def close_pool() -> None:
    _pool.close_all()
    log.info("🔌  DB connection pool đã đóng")


__all__ = [
    "save_to_db",
    "get_repo",
    "get_pool",
    "close_pool",
    "ConnectionPool",
    "HotelRepository",
]