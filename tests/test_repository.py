# tests/test_repository.py
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call
from db.repository import HotelRepository, ConnectionPool


class TestHotelRepository:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.mock_pool = MagicMock(spec=ConnectionPool)
        self.mock_conn = MagicMock()
        self.mock_cur = MagicMock()
        self.mock_pool.get.return_value = self.mock_conn
        self.mock_conn.cursor.return_value = self.mock_cur
        self.repo = HotelRepository(self.mock_pool)

    def test_get_or_create_khuvuc_existing(self):
        self.mock_cur.fetchone.return_value = (1,)
        result = self.repo.get_or_create_khuvuc(
            self.mock_cur, "Hà Nội"
        )
        assert result == 1

    def test_get_or_create_khuvuc_new(self):
        self.mock_cur.fetchone.side_effect = [None, (2,)]
        result = self.repo.get_or_create_khuvuc(
            self.mock_cur, "Hà Nội"
        )
        assert result == 2

    def test_get_or_create_coso_existing(self):
        self.mock_cur.fetchone.return_value = (10,)
        hotel = {
            "url": "https://booking.com/hotel/test",
            "name": "Test Hotel",
            "address": "123 Test St",
            "stars": 4,
            "type": "Khách Sạn",
            "platform": "booking.com",
        }
        result = self.repo.get_or_create_coso(
            self.mock_cur, 1, hotel
        )
        assert result == 10

    def test_insert_gia(self):
        self.repo.insert_gia(
            self.mock_cur,
            ma_loai=1,
            gia=500_000,
            con_phong=True,
            gia_goc=600_000,
        )
        self.mock_cur.execute.assert_called_once()
        call_args = self.mock_cur.execute.call_args[0]
        assert "INSERT INTO LichSuGia" in call_args[0]

    def test_insert_review_success(self):
        self.mock_cur.fetchone.return_value = None
        review = {
            "reviewer": "Nguyễn Văn A",
            "score": 8.5,
            "text": "Khách sạn rất tốt, phòng sạch sẽ",
            "date": "2024-03-15",
            "lang": "vi",
            "platform": "booking.com",
        }
        result = self.repo.insert_review(
            self.mock_cur, 1, review
        )
        assert result is True

    def test_insert_review_duplicate(self):
        self.mock_cur.fetchone.return_value = (1,)
        review = {
            "reviewer": "Nguyễn Văn A",
            "score":    8.5,
            "text":     "Khách sạn rất tốt",
            "date":     "2024-03-15",
            "lang":     "vi",
            "platform": "booking.com",
        }
        result = self.repo.insert_review(
            self.mock_cur, 1, review
        )
        assert result is False

    def test_insert_review_empty_text(self):
        review = {
            "reviewer": "Test User",
            "score":    5.0,
            "text":     "",
            "lang":     "vi",
            "platform": "booking.com",
        }
        result = self.repo.insert_review(
            self.mock_cur, 1, review
        )
        assert result is False

    def test_insert_review_score_normalization(self):
        """Score > 10 phải được chia 10."""
        self.mock_cur.fetchone.return_value = None
        review = {
            "reviewer": "Test User",
            "score":    85,   # thang 100 → phải thành 8.5
            "text":     "Khách sạn rất tốt, phòng sạch",
            "lang":     "vi",
            "platform": "booking.com",
        }
        result = self.repo.insert_review(
            self.mock_cur, 1, review
        )
        assert result is True
        call_args = self.mock_cur.execute.call_args_list[-1][0]
        # Kiểm tra score đã được normalize
        params = call_args[1]
        score_idx = 5  # vị trí SoDiem trong INSERT
        assert params[score_idx] <= 10.0

    def test_save_hotel_full_flow(self):
        """Test toàn bộ flow save_hotel."""
        self.mock_cur.fetchone.side_effect = [
            None,   # get_or_create_khuvuc → new
            (1,),   # INSERT KhuVuc → MaKhuVuc = 1
            None,   # get_or_create_coso → new
            (10,),  # INSERT CoSoLuuTru → MaCoSo = 10
            None,   # get_or_create_loaiphong → new
            (100,), # INSERT LoaiPhong → MaLoai = 100
            None,   # insert_review dedup check → not exists
        ]
        hotel = {
            "url":      "https://booking.com/hotel/test",
            "name":     "Test Hotel",
            "address":  "123 Test St",
            "city":     "Hà Nội",
            "stars":    4,
            "type":     "Khách Sạn",
            "platform": "booking.com",
        }
        rooms = [{
            "name":      "Phòng Đôi",
            "price":     500_000,
            "available": True,
        }]
        reviews = [{
            "reviewer": "Nguyễn Văn A",
            "score":    8.5,
            "text":     "Khách sạn rất tốt, phòng sạch sẽ",
            "date":     "2024-03-15",
            "lang":     "vi",
            "platform": "booking.com",
        }]
        stats = self.repo.save_hotel(hotel, rooms, reviews)
        assert "rooms_saved"   in stats
        assert "reviews_saved" in stats
        assert "reviews_skip"  in stats
        self.mock_conn.commit.assert_called_once()

    def test_save_hotel_rollback_on_error(self):
        """Test rollback khi có lỗi."""
        self.mock_cur.execute.side_effect = Exception("DB Error")
        hotel = {
            "url":      "https://booking.com/hotel/test",
            "name":     "Test Hotel",
            "city":     "Hà Nội",
            "stars":    4,
            "type":     "Khách Sạn",
            "platform": "booking.com",
        }
        with pytest.raises(Exception):
            self.repo.save_hotel(hotel, [], [])
        self.mock_conn.rollback.assert_called_once()

    def test_check_url_exists_true(self):
        self.mock_cur.fetchone.return_value = (1,)
        result = self.repo.check_url_exists(
            "https://booking.com/hotel/test"
        )
        assert result is True

    def test_check_url_exists_false(self):
        self.mock_cur.fetchone.return_value = None
        result = self.repo.check_url_exists(
            "https://booking.com/hotel/new"
        )
        assert result is False

    def test_get_hotel_count(self):
        self.mock_cur.fetchone.return_value = (42,)
        result = self.repo.get_hotel_count()
        assert result == 42

    def test_get_review_count(self):
        self.mock_cur.fetchone.return_value = (1000,)
        result = self.repo.get_review_count()
        assert result == 1000

    def test_save_reviews_batch(self):
        """Test batch insert reviews."""
        self.mock_cur.fetchone.return_value = None
        reviews = [
            {
                "reviewer": f"User {i}",
                "score":    float(i % 10),
                "text":     f"Review text {i} — khách sạn rất tốt",
                "lang":     "vi",
                "platform": "booking.com",
            }
            for i in range(25)
        ]
        stats = self.repo.save_reviews_batch(
            ma_coso    = 1,
            reviews    = reviews,
            batch_size = 10,
        )
        assert "saved" in stats
        assert "skip"  in stats
        assert "error" in stats
