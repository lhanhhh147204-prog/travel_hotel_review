# nlp/sentiment.py
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from typing import NamedTuple
from pathlib import Path
from dotenv import load_dotenv
from groq import Groq

log = logging.getLogger(__name__)
# ── Validate API key sớm, lỗi rõ ràng ───────────────────────
_CURRENT_DIR = Path(__file__).resolve().parent  # thư mục nlp/
_ROOT_DIR = _CURRENT_DIR.parent                # thư mục gốc PyCharmMiscProject/
_ENV_PATH = _ROOT_DIR / ".env"

# Ép load_dotenv tìm đúng file .env ở thư mục gốc
load_dotenv(dotenv_path=_ENV_PATH)

_GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not _GROQ_API_KEY:
    raise EnvironmentError(
        f"GROQ_API_KEY chưa được thiết lập tại: {_ENV_PATH}\n"
        "Hãy chắc chắn file .env nằm ở thư mục gốc dự án và có dòng:\n"
        "GROQ_API_KEY=gsk_LF3yjWJKAPzookEeji... (như trong ảnh bạn chụp)"
    )

# Debug an toàn: lấy 8 ký tự đầu từ biến đã load
log.debug("GROQ_API_KEY loaded (prefix: %s...)", _GROQ_API_KEY[:8])

_MODEL = "llama3-8b-8192"


# ── Lazy client (chỉ tạo khi cần, không crash lúc import) ────

_client: Groq | None = None

def _get_client() -> Groq:
    global _client
    if _client is None:
        _client = Groq(api_key=_GROQ_API_KEY)
    return _client


# ── Public data classes ───────────────────────────────────────

class SentimentLexicon:
    """Giữ lại để không phá vỡ các import hiện có."""
    VI_POS: set[str] = set()
    VI_NEG: set[str] = set()
    EN_POS: set[str] = set()
    EN_NEG: set[str] = set()
    KO_POS: set[str] = set()
    KO_NEG: set[str] = set()
    ZH_POS: set[str] = set()
    ZH_NEG: set[str] = set()
    NEGATION: set[str] = set()
    STRONG_POS_PHRASES: set[str] = set()
    STRONG_NEG_PHRASES: set[str] = set()


class SentimentResult(NamedTuple):
    label:      str
    pos_score:  float
    neg_score:  float
    confidence: float


_NEUTRAL = SentimentResult("Trung lập", 0.0, 0.0, 0.0)


# ── LLM prompt ───────────────────────────────────────────────

_SYSTEM_PROMPT = """Bạn là chuyên gia phân tích cảm xúc (sentiment analysis) cho đánh giá khách sạn.
Trả về JSON đúng cấu trúc (không markdown, không giải thích):

{
  "label": "Tích cực" | "Tiêu cực" | "Trung lập",
  "pos_score": <float 0-10>,
  "neg_score": <float 0-10>,
  "confidence": <float 0-1>
}

Quy tắc:
- Tích cực = khen ngợi
- Tiêu cực = phàn nàn
- Trung lập = không rõ
- Hiểu phủ định: "không tốt" = tiêu cực
- Hỗ trợ đa ngôn ngữ
"""


# ── LLM call ─────────────────────────────────────────────────

def _call_llm(text: str, lang: str = "vi") -> SentimentResult:
    global raw
    prompt = f"[lang={lang}]\n{text}"
    try:
        response = _get_client().chat.completions.create(
            model=_MODEL,
            temperature=0,
            max_tokens=200,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
        )
        raw  = response.choices[0].message.content.strip()
        # strip markdown code fences nếu model vô tình thêm vào
        raw  = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        return SentimentResult(
            label=      str(data.get("label",      "Trung lập")),
            pos_score=  float(data.get("pos_score",  0.0)),
            neg_score=  float(data.get("neg_score",  0.0)),
            confidence= float(data.get("confidence", 0.0)),
        )
    except json.JSONDecodeError as exc:
        log.warning("Groq trả về JSON không hợp lệ: %s | raw=%r", exc, raw)
        return _NEUTRAL
    except Exception as exc:
        log.error("Groq sentiment error: %s", exc)
        return _NEUTRAL


# ── SentimentAnalyzer ────────────────────────────────────────

class SentimentAnalyzer:

    @lru_cache(maxsize=10_000)
    def analyse(self, text: str, lang: str = "vi") -> SentimentResult:
        if not text or not text.strip():
            return _NEUTRAL
        return _call_llm(text, lang)

    def batch_analyse(
        self, texts: list[tuple[str, str]]
    ) -> list[SentimentResult]:
        return [self.analyse(text, lang) for text, lang in texts]


# ── Singleton ────────────────────────────────────────────────

_analyzer = SentimentAnalyzer()


def analyse_sentiment(text: str, lang: str = "vi") -> str:
    return _analyzer.analyse(text, lang).label


def analyse_sentiment_full(text: str, lang: str = "vi") -> SentimentResult:
    return _analyzer.analyse(text, lang)