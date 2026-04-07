# core/state.py
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


class ScrapeState:
    """
    Lưu trạng thái scrape vào file để resume
    khi bị gián đoạn.
    """

    STATE_FILE = Path("scrape_state.json")

    def __init__(self):
        self._lock   = asyncio.Lock()
        self._done:   set[str]        = set()
        self._failed: dict[str, str]  = {}
        self._load()

    def _load(self) -> None:
        if self.STATE_FILE.exists():
            try:
                data = json.loads(
                    self.STATE_FILE.read_text("utf-8")
                )
                self._done   = set(data.get("done",   []))
                self._failed = data.get("failed", {})
                log.info(
                    f"📂  Resume: {len(self._done)} done, "
                    f"{len(self._failed)} failed"
                )
            except Exception as e:
                log.warning(f"Load state lỗi: {e}")

    def _save(self) -> None:
        self.STATE_FILE.write_text(
            json.dumps(
                {
                    "done":    list(self._done),
                    "failed":  self._failed,
                    "updated": datetime.now().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def should_skip(self, url: str) -> bool:
        """Bỏ qua URL đã xử lý thành công."""
        return url in self._done

    async def mark_ok(self, url: str) -> None:
        async with self._lock:
            self._done.add(url)
            self._failed.pop(url, None)
            self._save()

    async def mark_fail(self, url: str, reason: str) -> None:
        async with self._lock:
            self._failed[url] = reason
            self._save()

    @property
    def stats(self) -> dict:
        return {
            "done":   len(self._done),
            "failed": len(self._failed),
        }