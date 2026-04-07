# tests/test_sentiment.py
from __future__ import annotations

import pytest
from nlp.sentiment import (
    SentimentAnalyzer,
    analyse_sentiment,
    analyse_sentiment_full,
)


class TestSentimentAnalyzer:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.analyzer = SentimentAnalyzer()

    def test_positive_vi(self):
        result = self.analyzer.analyse(
            "Khách sạn rất tuyệt vời, nhân viên thân thiện", "vi"
        )
        assert result.label == "Tích cực"
        assert result.pos_score > result.neg_score

    def test_negative_vi(self):
        result = self.analyzer.analyse(
            "Phòng rất bẩn, dịch vụ tệ, thất vọng lắm", "vi"
        )
        assert result.label == "Tiêu cực"
        assert result.neg_score > result.pos_score

    def test_neutral_vi(self):
        result = self.analyzer.analyse(
            "Khách sạn ở trung tâm thành phố", "vi"
        )
        assert result.label in ["Trung lập", "Tích cực", "Tiêu cực"]

    def test_positive_en(self):
        result = self.analyzer.analyse(
            "Excellent hotel, very clean and friendly staff", "en"
        )
        assert result.label == "Tích cực"

    def test_negative_en(self):
        result = self.analyzer.analyse(
            "Terrible experience, dirty room, rude staff", "en"
        )
        assert result.label == "Tiêu cực"

    def test_negation_vi(self):
        result = self.analyzer.analyse(
            "Không tệ chút nào, khá ổn", "vi"
        )
        assert result.label in ["Tích cực", "Trung lập"]

    def test_empty_text(self):
        result = self.analyzer.analyse("", "vi")
        assert result.label == "Trung lập"
        assert result.confidence == 0.0

    def test_strong_positive(self):
        result = self.analyzer.analyse(
            "Tuyệt vời lắm! 5 sao! Sẽ quay lại!", "vi"
        )
        assert result.label == "Tích cực"
        assert result.confidence > 0.5

    def test_strong_negative(self):
        result = self.analyzer.analyse(
            "Tệ nhất từ trước đến nay! Never again!", "en"
        )
        assert result.label == "Tiêu cực"

    def test_confidence_range(self):
        result = self.analyzer.analyse(
            "Khách sạn tốt", "vi"
        )
        assert 0.0 <= result.confidence <= 1.0

    def test_batch_analyse(self):
        texts = [
            ("Rất tốt", "vi"),
            ("Very bad", "en"),
            ("Bình thường", "vi"),
        ]
        results = self.analyzer.batch_analyse(texts)
        assert len(results) == 3

    def test_wrapper_function(self):
        label = analyse_sentiment("Khách sạn đẹp", "vi")
        assert label in ["Tích cực", "Tiêu cực", "Trung lập"]

    def test_full_wrapper_function(self):
        result = analyse_sentiment_full("Rất tệ", "vi")
        assert hasattr(result, "label")
        assert hasattr(result, "pos_score")
        assert hasattr(result, "neg_score")
        assert hasattr(result, "confidence")