# core/behavior.py
from __future__ import annotations

import asyncio
import random

from playwright.async_api import Page


class HumanBehavior:
    """Mô phỏng hành vi người dùng thật."""

    @staticmethod
    async def move_mouse_naturally(page: Page) -> None:
        """Di chuyển chuột theo đường cong Bezier."""
        vp = page.viewport_size or {"width": 1366, "height": 768}
        x0 = random.randint(100, vp["width"]  - 100)
        y0 = random.randint(100, vp["height"] - 100)
        x1 = random.randint(100, vp["width"]  - 100)
        y1 = random.randint(100, vp["height"] - 100)
        cx = random.randint(min(x0, x1) - 50, max(x0, x1) + 50)
        cy = random.randint(min(y0, y1) - 50, max(y0, y1) + 50)

        steps = random.randint(15, 30)
        for i in range(steps + 1):
            t  = i / steps
            bx = int((1-t)**2 * x0 + 2*(1-t)*t * cx + t**2 * x1)
            by = int((1-t)**2 * y0 + 2*(1-t)*t * cy + t**2 * y1)
            await page.mouse.move(bx, by)
            await asyncio.sleep(random.uniform(0.01, 0.04))

    @staticmethod
    async def human_scroll(page: Page) -> None:
        """Cuộn trang tự nhiên — có lúc cuộn ngược lên."""
        for _ in range(random.randint(3, 8)):
            direction = -1 if random.random() < 0.2 else 1
            distance  = random.randint(150, 600) * direction
            await page.evaluate(f"window.scrollBy(0, {distance})")
            await asyncio.sleep(random.uniform(0.4, 1.2))
            if random.random() < 0.3:
                await asyncio.sleep(random.uniform(1.5, 3.0))

    @staticmethod
    async def random_click_empty_area(page: Page) -> None:
        """Click vào vùng trống để tránh bị detect là bot."""
        vp = page.viewport_size or {"width": 1366, "height": 768}
        x  = random.randint(50, vp["width"]  - 50)
        y  = random.randint(50, vp["height"] - 50)
        await page.mouse.click(x, y)
        await asyncio.sleep(random.uniform(0.2, 0.5))

    @staticmethod
    async def simulate_reading(
            page: Page,
            min_sec: float = 3.0,
            max_sec: float = 8.0,
    ) -> None:
        """Giả lập thời gian đọc trang."""
        read_time = random.uniform(min_sec, max_sec)
        chunks    = max(1, int(read_time / 0.5))
        for _ in range(chunks):
            await page.evaluate(
                f"window.scrollBy(0, {random.randint(30, 100)})"
            )
            await asyncio.sleep(0.5)

    @staticmethod
    async def type_like_human(
            page: Page,
            selector: str,
            text: str,
    ) -> None:
        """Gõ phím với tốc độ và lỗi như người thật."""
        el = page.locator(selector).first
        await el.click()
        await asyncio.sleep(random.uniform(0.3, 0.7))
        for char in text:
            await el.type(char, delay=random.randint(80, 200))
            if random.random() < 0.05:
                wrong = random.choice("abcdefghijklmnopqrstuvwxyz")
                await el.type(wrong, delay=random.randint(50, 100))
                await asyncio.sleep(random.uniform(0.2, 0.5))
                await page.keyboard.press("Backspace")
                await asyncio.sleep(random.uniform(0.1, 0.3))