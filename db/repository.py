# db/repository.py
from __future__ import annotations

import hashlib
import logging
import threading
from datetime import datetime
from typing import Any

import pyodbc

from core.helpers import clean, parse_date, get_region, md5_hash
from nlp.sentiment import analyse_sentiment, analyse_sentiment_full
from nlp.language_detect import detect_language, detect_guest_type

log = logging.getLogger(__name__)


class ConnectionPool:
    """Thread-safe connection pool cho pyodbc."""

    def __init__(self, conn_str: str, pool_size: int = 5):
        self._conn_str = conn_str
        self._pool_size = pool_size
        self._pool: list[pyodbc.Connection] = []
        self._lock = threading.Lock()
        self._timeout = 30

    def _create_conn(self) -> pyodbc.Connection:
        conn = pyodbc.connect(
            self._conn_str,
            autocommit=False,
            timeout=self._timeout,
        )
        conn.execute("SET QUERY_GOVERNOR_COST_LIMIT 60000")
        return conn

    def get(self) -> pyodbc.Connection:
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
                try:
                    conn.execute("SELECT 1")
                    return conn
                except Exception:
                    pass
            return self._create_conn()

    def release(self, conn: pyodbc.Connection) -> None:
        with self._lock:
            if len(self._pool) < self._pool_size:
                self._pool.append(conn)
            else:
                try:
                    conn.close()
                except Exception:
                    pass

    def close_all(self) -> None:
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()


class HotelRepository:
    """Repository pattern — tách biệt logic DB khỏi scraping."""

    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    # ── KhuVuc ────────────────────────────────────────────────
    def get_or_create_khuvuc(
            self,
            cur: pyodbc.Cursor,
            tinh: str,
    ) -> int:
        tinh = tinh.strip()
        cur.execute(
            "SELECT MaKhuVuc FROM KhuVuc WHERE TinhThanh = ?",
            tinh,
        )
        row = cur.fetchone()
        if row:
            return row[0]

        vung = get_region(tinh)
        cur.execute(
            """
            INSERT INTO KhuVuc (TinhThanh, VungMien)
            OUTPUT INSERTED.MaKhuVuc
            VALUES (?, ?)
            """,
            tinh, vung,
        )
        return cur.fetchone()[0]

    # ── CoSoLuuTru ────────────────────────────────────────────
    def get_or_create_coso(
            self,
            cur: pyodbc.Cursor,
            ma_kv: int,
            hotel: dict,
    ) -> int:
        url_hash = md5_hash(hotel["url"])[:12].upper()

        cur.execute(
            "SELECT MaCoSo FROM CoSoLuuTru WHERE MoTa = ?",
            url_hash,
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE CoSoLuuTru
                SET TenCoSo       = ?,
                    SoSao         = ?,
                    DiaChiChiTiet = ?,
                    LoaiCoSo      = ?,
                    UpdatedAt     = GETDATE()
                WHERE MaCoSo = ?
                """,
                hotel["name"],
                hotel.get("stars"),
                hotel.get("address"),
                hotel.get("type", "Khách Sạn"),
                row[0],
            )
            return row[0]

        cur.execute(
            """
            INSERT INTO CoSoLuuTru
                (MaKhuVuc, TenCoSo, DiaChiChiTiet,
                 SoSao, LoaiCoSo, MoTa, NguonDuLieu, UrlGoc)
            OUTPUT INSERTED.MaCoSo
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ma_kv,
            hotel["name"],
            hotel.get("address"),
            hotel.get("stars"),
            hotel.get("type", "Khách Sạn"),
            url_hash,
            hotel["platform"],
            hotel["url"][:1000],
        )
        return cur.fetchone()[0]

    # ── LoaiPhong_Ve ──────────────────────────────────────────
    def get_or_create_loaiphong(
            self,
            cur: pyodbc.Cursor,
            ma_coso: int,
            ten_loai: str,
    ) -> int:
        cur.execute(
            """
            SELECT MaLoai FROM LoaiPhong_Ve
            WHERE MaCoSo = ? AND TenLoai = ?
            """,
            ma_coso, ten_loai,
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            """
            INSERT INTO LoaiPhong_Ve (MaCoSo, TenLoai)
            OUTPUT INSERTED.MaLoai
            VALUES (?, ?)
            """,
            ma_coso, ten_loai,
        )
        return cur.fetchone()[0]

    # ── LichSuGia ─────────────────────────────────────────────
    def insert_gia(
            self,
            cur: pyodbc.Cursor,
            ma_loai: int,
            gia: float,
            con_phong: bool = True,
            gia_goc: float | None = None,
    ) -> None:
        """Chèn bản ghi giá mới — luôn insert không update."""
        phan_tram_giam = None
        if gia_goc and gia_goc > gia:
            phan_tram_giam = round(
                (gia_goc - gia) / gia_goc * 100, 1
            )

        cur.execute(
            """
            INSERT INTO LichSuGia
                (MaLoai, GiaHienTai, GiaGoc,
                 PhanTramGiam, ConPhong, NgayCheck)
            VALUES (?, ?, ?, ?, ?, GETDATE())
            """,
            ma_loai,
            gia,
            gia_goc,
            phan_tram_giam,
            1 if con_phong else 0,
        )

    # ── DanhGia ───────────────────────────────────────────────
    def insert_review(
            self,
            cur: pyodbc.Cursor,
            ma_coso: int,
            r: dict,
    ) -> bool:
        """
        Chèn đánh giá mới.
        Returns True nếu insert thành công, False nếu duplicate.
        """
        ten = (r.get("reviewer") or "Ẩn danh")[:200]
        text = (r.get("text") or "")[:4000]

        if not text.strip():
            return False

        # ── Phát hiện ngôn ngữ ────────────────────────────────
        lang = r.get("lang") or detect_language(text)
        loai_khach = r.get("guest_type") or detect_guest_type(text)

        # ── Phân tích cảm xúc ─────────────────────────────────
        sentiment_result = analyse_sentiment_full(text, lang)
        nhan_cam_xuc = sentiment_result.label

        # ── Điểm số ───────────────────────────────────────────
        try:
            diem = float(r.get("score") or 0)
            if diem > 10:
                diem = diem / 10
            diem = round(min(max(diem, 0), 10), 1)
        except (ValueError, TypeError):
            diem = 0.0

        # ── Ngày đánh giá ─────────────────────────────────────
        ngay_str = parse_date(r.get("date"))
        ngay_dt = None
        if ngay_str:
            try:
                ngay_dt = datetime.strptime(ngay_str, "%Y-%m-%d")
            except ValueError:
                pass

        # ── Dedup bằng MD5 hash ───────────────────────────────
        review_hash = hashlib.md5(
            f"{ma_coso}|{ten}|{text[:500]}".encode()
        ).hexdigest()

        cur.execute(
            "SELECT 1 FROM DanhGia WHERE ReviewHash = ?",
            review_hash,
        )
        if cur.fetchone():
            return False  # duplicate

        # ── Insert ────────────────────────────────────────────
        cur.execute(
            """
            INSERT INTO DanhGia (
                MaCoSo, TenKhachHang, QuocTich,
                NgonNgu, LoaiKhach,
                SoDiem, NoiDungBinhLuan, TieuDe,
                NgayDanhGia, LoaiPhongDaO,
                NhanCamXuc, ReviewHash, NguonDuLieu
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ma_coso,
            ten,
            clean(r.get("country"), 100),
            lang[:10],
            loai_khach[:50] if loai_khach else None,
            diem,
            text,
            clean(r.get("title"), 500),
            ngay_dt,
            clean(r.get("room"), 200),
            nhan_cam_xuc,
            review_hash,
            r.get("platform", "unknown")[:100],
        )
        return True

    # ── Batch save toàn bộ ────────────────────────────────────
    def save_hotel(
            self,
            hotel: dict,
            rooms: list[dict],
            reviews: list[dict],
    ) -> dict[str, int]:
        """
        Lưu toàn bộ dữ liệu 1 khách sạn vào DB.
        Returns: thống kê số bản ghi đã lưu.
        """
        stats = {
            "rooms_saved": 0,
            "reviews_saved": 0,
            "reviews_skip": 0,
        }

        conn = self._pool.get()
        try:
            cur = conn.cursor()

            # ── KhuVuc & CoSo ─────────────────────────────────
            ma_kv = self.get_or_create_khuvuc(cur, hotel["city"])
            ma_coso = self.get_or_create_coso(cur, ma_kv, hotel)

            # ── Giá phòng ─────────────────────────────────────
            for rm in rooms:
                ten_loai = (rm.get("name") or "Phòng Tiêu Chuẩn")[:200]
                gia = rm.get("price")
                if gia:
                    ma_loai = self.get_or_create_loaiphong(
                        cur, ma_coso, ten_loai
                    )
                    self.insert_gia(
                        cur,
                        ma_loai,
                        gia,
                        rm.get("available", True),
                        rm.get("original_price"),
                    )
                    stats["rooms_saved"] += 1

            # ── Đánh giá ──────────────────────────────────────
            for r in reviews:
                ok = self.insert_review(cur, ma_coso, r)
                if ok:
                    stats["reviews_saved"] += 1
                else:
                    stats["reviews_skip"] += 1

            # ── Cập nhật điểm trung bình ──────────────────────
            cur.execute(
                """
                UPDATE CoSoLuuTru
                SET DiemTrungBinh = (
                    SELECT AVG(CAST(SoDiem AS FLOAT))
                    FROM DanhGia
                    WHERE MaCoSo = ? AND SoDiem > 0
                ),
                TongSoReview = (
                    SELECT COUNT(*)
                    FROM DanhGia
                    WHERE MaCoSo = ?
                ),
                UpdatedAt = GETDATE()
                WHERE MaCoSo = ?
                """,
                ma_coso, ma_coso, ma_coso,
            )

            conn.commit()
            return stats

        except Exception as e:
            conn.rollback()
            log.error(f"DB error: {e}", exc_info=True)
            raise
        finally:
            self._pool.release(conn)

    # ── Batch insert reviews (streaming) ─────────────────────
    def save_reviews_batch(
            self,
            ma_coso: int,
            reviews: list[dict],
            batch_size: int = 50,
    ) -> dict[str, int]:
        """Lưu reviews theo batch nhỏ — tránh transaction quá lớn."""
        stats = {"saved": 0, "skip": 0, "error": 0}

        for i in range(0, len(reviews), batch_size):
            batch = reviews[i: i + batch_size]
            conn = self._pool.get()
            try:
                cur = conn.cursor()
                for r in batch:
                    try:
                        ok = self.insert_review(cur, ma_coso, r)
                        if ok:
                            stats["saved"] += 1
                        else:
                            stats["skip"] += 1
                    except Exception as e:
                        stats["error"] += 1
                        log.debug(f"Review insert error: {e}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                log.warning(f"Batch {i} rollback: {e}")
            finally:
                self._pool.release(conn)

        return stats

    # ── Query helpers ─────────────────────────────────────────
    def get_hotel_count(self) -> int:
        conn = self._pool.get()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM CoSoLuuTru")
            return cur.fetchone()[0]
        finally:
            self._pool.release(conn)

    def get_review_count(self) -> int:
        conn = self._pool.get()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM DanhGia")
            return cur.fetchone()[0]
        finally:
            self._pool.release(conn)

    def get_progress_by_province(self) -> list[dict]:
        """Tiến độ scrape theo từng tỉnh."""
        conn = self._pool.get()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    kv.TinhThanh,
                    kv.VungMien,
                    COUNT(DISTINCT cs.MaCoSo) AS SoKhachSan,
                    COUNT(dg.MaDanhGia)       AS SoDanhGia,
                    MAX(dg.CreatedAt)         AS LanCuoiCapNhat
                FROM KhuVuc kv
                LEFT JOIN CoSoLuuTru cs ON cs.MaKhuVuc = kv.MaKhuVuc
                LEFT JOIN DanhGia    dg ON dg.MaCoSo   = cs.MaCoSo
                GROUP BY kv.TinhThanh, kv.VungMien
                ORDER BY SoDanhGia DESC
                """
            )
            cols = [d[0] for d in cur.description]
            return [
                dict(zip(cols, row))
                for row in cur.fetchall()
            ]
        finally:
            self._pool.release(conn)

    def check_url_exists(self, url: str) -> bool:
        """Kiểm tra URL đã được scrape chưa."""
        url_hash = md5_hash(url)[:12].upper()
        conn = self._pool.get()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM CoSoLuuTru WHERE MoTa = ?",
                url_hash,
            )
            return cur.fetchone() is not None
        finally:
            self._pool.release(conn)
