# config.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass
class ScraperConfig:
    # ── Concurrency ───────────────────────────────────────
    max_concurrent: int = 3
    browser_restart_each: int = 100
    max_pages_per_hotel: int = 10
    reviews_per_page: int = 10
    retry_limit: int = 3

    # ── Delays (giây) ────────────────────────────────────
    delay_min: float = 2.5
    delay_max: float = 6.0
    page_delay_min: float = 1.5
    page_delay_max: float = 3.5
    batch_pause_min: float = 15.0
    batch_pause_max: float = 30.0

    # ── Paths ─────────────────────────────────────────────
    input_file: Path = Path("hotel_urls_collected.txt")
    state_file: Path = Path("scrape_state.txt")
    cookie_dir: Path = Path("cookies")
    log_file: Path = Path("logs/etl.log")
    report_dir: Path = Path("reports")

    # ── Database ──────────────────────────────────────────
    db_server: str = field(
        default_factory=lambda: os.getenv("DB_SERVER", "localhost")
    )
    db_name: str = field(
        default_factory=lambda: os.getenv("DB_NAME", "BIG_DATA")
    )
    db_trusted: bool = True

    @property
    def db_conn_str(self) -> str:
        if self.db_trusted:
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.db_server};"
                f"DATABASE={self.db_name};"
                f"Trusted_Connection=yes;"
            )
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_server};"
            f"DATABASE={self.db_name};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )

    def __post_init__(self):
        for d in [self.cookie_dir, self.log_file.parent, self.report_dir]:
            d.mkdir(parents=True, exist_ok=True)


# Singleton config
CFG = ScraperConfig()