# tests/test_language_detect.py
from __future__ import annotations

import pytest
from nlp.language_detect import (
    LanguageDetector,
    detect_language,
    detect_guest_type,
)


class TestLanguageDetector:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.detector = LanguageDetector()

    def test_detect_vietnamese(self):
        text = "Khách sạn rất đẹp, phòng sạch sẽ và thoáng mát"
        assert self.detector.detect(text) == "vi"

    def test_detect_english(self):
        text = "The hotel was excellent, very clean and comfortable"
        assert self.detector.detect(text) == "en"

    def test_detect_chinese(self):
        text = "酒店非常好，房间干净整洁"
        assert self.detector.detect(text) == "zh"

    def test_detect_korean(self):
        text = "호텔이 매우 좋았습니다. 깨끗하고 친절했어요"
        assert self.detector.detect(text) == "ko"

    def test_detect_japanese(self):
        text = "ホテルはとても良かったです。清潔で快適でした"
        assert self.detector.detect(text) == "ja"

    def test_detect_arabic(self):
        text = "الفندق رائع جداً ونظيف ومريح"
        assert self.detector.detect(text) == "ar"

    def test_detect_russian(self):
        text = "Отель очень хороший, чистый и удобный"
        assert self.detector.detect(text) == "ru"

    def test_detect_thai(self):
        text = "โรงแรมดีมาก สะอาดและสะดวกสบาย"
        assert self.detector.detect(text) == "th"

    def test_empty_text(self):
        result = self.detector.detect("")
        assert result == "vi"

    def test_short_text(self):
        result = self.detector.detect("ok")
        assert isinstance(result, str)

    def test_guest_type_vietnamese(self):
        text = "Phòng sạch sẽ, nhân viên thân thiện"
        result = self.detector.detect_guest_type(text)
        assert result == "Khách Việt"

    def test_guest_type_korean(self):
        text = "호텔이 매우 좋았습니다"
        result = self.detector.detect_guest_type(text)
        assert result == "Khách Hàn Quốc"

    def test_guest_type_chinese(self):
        text = "酒店非常好"
        result = self.detector.detect_guest_type(text)
        assert result == "Khách Trung Quốc"

    def test_batch_detect(self):
        texts = [
            "Khách sạn đẹp",
            "Very good hotel",
            "酒店很好",
        ]
        results = self.detector.batch_detect(texts)
        assert len(results) == 3
        assert results[0] == "vi"
        assert results[1] == "en"
        assert results[2] == "zh"

    def test_wrapper_detect_language(self):
        result = detect_language("Phòng sạch sẽ")
        assert result == "vi"

    def test_wrapper_detect_guest_type(self):
        result = detect_guest_type("Phòng sạch sẽ")
        assert result == "Khách Việt"

    def test_cache_works(self):
        text = "Khách sạn rất tốt"
        r1   = self.detector.detect(text)
        r2   = self.detector.detect(text)
        assert r1 == r2