# core/session.py
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from playwright.async_api import BrowserContext

log = logging.getLogger(__name__)


class SessionManager:
    """
    Quản lý session/cookie để duy trì trạng thái
    đăng nhập và tránh bị detect là bot mới.
    """

    COOKIE_DIR = Path("cookies")

    def __init__(self):
        self.COOKIE_DIR.mkdir(exist_ok=True)

    def _cookie_path(self, domain: str) -> Path:
        safe = re.sub(r"[^\w]", "_", domain)
        return self.COOKIE_DIR / f"{safe}.json"

    async def save_cookies(
            self,
            ctx: BrowserContext,
            domain: str,
    ) -> None:
        cookies = await ctx.cookies()
        path    = self._cookie_path(domain)
        path.write_text(
            json.dumps(cookies, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(
            f"💾  Đã lưu {len(cookies)} cookies cho {domain}"
        )

    async def load_cookies(
            self,
            ctx: BrowserContext,
            domain: str,
    ) -> bool:
        path = self._cookie_path(domain)
        if not path.exists():
            return False
        try:
            cookies = json.loads(path.read_text("utf-8"))
            await ctx.add_cookies(cookies)
            log.info(
                f"🍪  Đã load {len(cookies)} cookies cho {domain}"
            )
            return True
        except Exception as e:
            log.warning(f"Load cookie lỗi: {e}")
            return False

    async def rotate_session(
            self,
            ctx: BrowserContext,
            domain: str,
    ) -> None:
        """Xóa cookie cũ, tạo session mới."""
        await ctx.clear_cookies()
        path = self._cookie_path(domain)
        if path.exists():
            path.unlink()
        log.info(f"🔄  Đã rotate session cho {domain}")