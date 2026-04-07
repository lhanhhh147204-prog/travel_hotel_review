# nlp/language_detect.py
from __future__ import annotations

import os
import json
import logging
from functools import lru_cache

import anthropic

log = logging.getLogger(__name__)

# ── Anthropic client ──────────────────────────────────────────
_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
_MODEL  = "claude-haiku-4-5-20251001"


# ── Prompt ────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Bạn là chuyên gia nhận diện ngôn ngữ cho đánh giá khách sạn.
Nhiệm vụ: xác định ngôn ngữ của đoạn văn bản và trả về JSON (KHÔNG markdown, KHÔNG giải thích):

{
  "lang": "<mã ngôn ngữ>"
}

Mã ngôn ngữ hợp lệ: vi, en, zh, ko, ja, fr, de, ru, th, ms, id, es, it, pt, ar, nl, pl, sv
Nếu không xác định được → trả về "en".
""".strip()

_LANG_TO_GUEST: dict[str, str] = {
    "vi": "Khách Việt",
    "en": "Khách Anh/Mỹ",
    "zh": "Khách Trung Quốc",
    "ko": "Khách Hàn Quốc",
    "ja": "Khách Nhật Bản",
    "fr": "Khách Pháp",
    "de": "Khách Đức",
    "ru": "Khách Nga",
    "th": "Khách Thái Lan",
    "ms": "Khách Malaysia",
    "id": "Khách Indonesia",
    "es": "Khách Tây Ban Nha",
    "it": "Khách Ý",
    "pt": "Khách Bồ Đào Nha",
    "ar": "Khách Ả Rập",
    "nl": "Khách Hà Lan",
}


def _call_llm(text: str) -> str:
    """Gọi Claude API để detect ngôn ngữ, trả về mã ngôn ngữ."""
    try:
        response = _client.messages.create(
            model      = _MODEL,
            max_tokens = 64,
            system     = _SYSTEM_PROMPT,
            messages   = [{"role": "user", "content": text}],
        )
        raw  = response.content[0].text.strip()
        data = json.loads(raw)
        return data.get("lang", "en")
    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        log.error("LLM parse error: %s | raw=%s", exc, locals().get("raw", ""))
        return "en"
    except anthropic.APIError as exc:
        log.error("Anthropic API error: %s", exc)
        return "en"


# ── LanguageDetector ──────────────────────────────────────────

class LanguageDetector:
    """
    Phát hiện ngôn ngữ review để phân loại khách Việt vs quốc tế.
    Interface hoàn toàn tương thích với phiên bản heuristic cũ.
    """

    @lru_cache(maxsize=50_000)
    def detect(self, text: str) -> str:
        """
        Phát hiện ngôn ngữ.
        Returns: mã ngôn ngữ chuẩn hóa (vi/en/zh/ko/ja/...)
        """
        if not text or len(text.strip()) < 5:
            return "vi"
        return _call_llm(text.strip())

    def detect_guest_type(self, text: str) -> str:
        """Phát hiện loại khách từ nội dung review."""
        lang = self.detect(text)
        return _LANG_TO_GUEST.get(lang, "Khách Quốc Tế Khác")

    def batch_detect(self, texts: list[str]) -> list[str]:
        """Phát hiện ngôn ngữ hàng loạt."""
        return [self.detect(t) for t in texts]


# ── Singleton & wrapper functions (giữ nguyên signature) ──────
_detector = LanguageDetector()


def detect_language(text: str) -> str:
    return _detector.detect(text)


def detect_guest_type(text: str) -> str:
    return _detector.detect_guest_type(text)
