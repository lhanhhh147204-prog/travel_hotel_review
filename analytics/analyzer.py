# analytics/analyzer.py
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc

log = logging.getLogger(__name__)


class DataAnalyzer:
    """
    Phân tích dữ liệu du lịch sau khi ETL hoàn tất.
    Xuất báo cáo Excel cho từng tỉnh thành.
    """

    def __init__(self, conn_str: str):
        self.conn_str   = conn_str
        self.output_dir = Path("reports")
        self.output_dir.mkdir(exist_ok=True)

    def _query(
            self,
            sql:    str,
            params: tuple = (),
    ) -> pd.DataFrame:
        with pyodbc.connect(self.conn_str) as conn:
            return pd.read_sql(sql, conn, params=params)

    # ── Report 00: Tổng hợp cuối ─────────────────────────────
    def report_tong_hop_cuoi(self) -> pd.DataFrame:
        sql = """
        SELECT
            kv.TinhThanh,
            kv.VungMien,
            kv.Tier,
            COUNT(DISTINCT cs.MaCoSo)                AS TongCoSo,
            COUNT(DISTINCT CASE WHEN cs.LoaiCoSo = N'Khách Sạn'
                           THEN cs.MaCoSo END)        AS SoKhachSan,
            COUNT(DISTINCT CASE WHEN cs.LoaiCoSo = N'Resort'
                           THEN cs.MaCoSo END)        AS SoResort,
            COUNT(DISTINCT CASE WHEN cs.LoaiCoSo = N'Homestay'
                           THEN cs.MaCoSo END)        AS SoHomestay,
            COUNT(dg.MaDanhGia)                      AS TongDanhGia,
            SUM(CASE WHEN dg.NgonNgu = 'vi'
                     THEN 1 ELSE 0 END)              AS KhachViet,
            SUM(CASE WHEN dg.NgonNgu != 'vi'
                     THEN 1 ELSE 0 END)              AS KhachQuocTe,
            ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB_Toan,
            ROUND(AVG(CASE WHEN dg.NgonNgu = 'vi'
                      THEN CAST(dg.SoDiem AS FLOAT)
                      END), 2)                        AS DiemTB_Viet,
            ROUND(AVG(CASE WHEN dg.NgonNgu != 'vi'
                      THEN CAST(dg.SoDiem AS FLOAT)
                      END), 2)                        AS DiemTB_QuocTe,
            ROUND(
                SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                         THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(dg.MaDanhGia), 0) * 100
            , 1)                                     AS TyLeTichCuc_Pct,
            ROUND(AVG(lg.GiaHienTai), 0)             AS GiaTB_VND,
            ROUND(MIN(lg.GiaHienTai), 0)             AS GiaMin_VND,
            ROUND(MAX(lg.GiaHienTai), 0)             AS GiaMax_VND,
            RANK() OVER (
                ORDER BY AVG(CAST(dg.SoDiem AS FLOAT)) DESC
            )                                        AS XepHang_DiemTB,
            RANK() OVER (
                ORDER BY COUNT(dg.MaDanhGia) DESC
            )                                        AS XepHang_LuotDanhGia
        FROM KhuVuc kv
        LEFT JOIN CoSoLuuTru   cs ON cs.MaKhuVuc = kv.MaKhuVuc
        LEFT JOIN DanhGia      dg ON dg.MaCoSo   = cs.MaCoSo
        LEFT JOIN LoaiPhong_Ve lp ON lp.MaCoSo   = cs.MaCoSo
        LEFT JOIN LichSuGia    lg ON lg.MaLoai   = lp.MaLoai
        GROUP BY kv.TinhThanh, kv.VungMien, kv.Tier
        ORDER BY DiemTB_Toan DESC
        """
        df = self._query(sql)

        with pd.ExcelWriter(
                self.output_dir / "00_BAO_CAO_TONG_HOP.xlsx",
                engine="openpyxl",
        ) as writer:
            df.to_excel(writer, sheet_name="Toàn Quốc", index=False)

            for vung in ["Miền Bắc", "Miền Trung", "Miền Nam"]:
                df_vung = df[df["VungMien"] == vung]
                if not df_vung.empty:
                    df_vung.to_excel(
                        writer, sheet_name=vung, index=False
                    )

            df.nlargest(10, "TongDanhGia").to_excel(
                writer, sheet_name="Top 10 Tỉnh", index=False
            )

            summary = pd.DataFrame([{
                "Tổng cơ sở lưu trú": df["TongCoSo"].sum(),
                "Tổng đánh giá":      df["TongDanhGia"].sum(),
                "Khách Việt":         df["KhachViet"].sum(),
                "Khách Quốc Tế":      df["KhachQuocTe"].sum(),
                "Điểm TB toàn quốc":  round(df["DiemTB_Toan"].mean(), 2),
                "Tỷ lệ tích cực %":   round(df["TyLeTichCuc_Pct"].mean(), 1),
                "Giá TB (VND)":       round(df["GiaTB_VND"].mean(), 0),
            }])
            summary.to_excel(
                writer, sheet_name="Tổng Kết", index=False
            )

        log.info(
            f"  ✅ Xuất: 00_BAO_CAO_TONG_HOP.xlsx "
            f"({len(df)} tỉnh thành)"
        )
        return df

    # ── Report 01: Tổng quan 63 tỉnh ─────────────────────────
    def report_tong_quan(self) -> pd.DataFrame:
        sql = """
        SELECT
            kv.VungMien,
            kv.TinhThanh,
            kv.Tier,
            COUNT(DISTINCT cs.MaCoSo)               AS SoCoSo,
            COUNT(dg.MaDanhGia)                     AS TongDanhGia,
            SUM(CASE WHEN dg.NgonNgu = 'vi'
                     THEN 1 ELSE 0 END)             AS KhachViet,
            SUM(CASE WHEN dg.NgonNgu != 'vi'
                     THEN 1 ELSE 0 END)             AS KhachQuocTe,
            ROUND(AVG(CAST(dg.SoDiem AS FLOAT)),2)  AS DiemTB,
            ROUND(AVG(lg.GiaHienTai), 0)            AS GiaTB_VND,
            ROUND(
                SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                         THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(dg.MaDanhGia), 0) * 100
            , 1)                                    AS TyLeTichCuc_Pct
        FROM KhuVuc kv
        LEFT JOIN CoSoLuuTru   cs ON cs.MaKhuVuc = kv.MaKhuVuc
        LEFT JOIN DanhGia      dg ON dg.MaCoSo   = cs.MaCoSo
        LEFT JOIN LoaiPhong_Ve lp ON lp.MaCoSo   = cs.MaCoSo
        LEFT JOIN LichSuGia    lg ON lg.MaLoai   = lp.MaLoai
        GROUP BY kv.VungMien, kv.TinhThanh, kv.Tier
        ORDER BY kv.VungMien, DiemTB DESC
        """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "01_tong_quan_63_tinh.xlsx",
            index=False,
            sheet_name="Tổng Quan",
        )
        log.info(
            f"  ✅ Xuất: 01_tong_quan_63_tinh.xlsx ({len(df)} dòng)"
        )
        return df

    # ── Report 02: Khách Việt vs Quốc Tế ─────────────────────
    def report_khach_viet_vs_quocte(self) -> pd.DataFrame:
        sql = """
        SELECT
            kv.TinhThanh,
            dg.LoaiKhach,
            dg.NgonNgu,
            COUNT(*)                                AS SoDanhGia,
            ROUND(AVG(CAST(dg.SoDiem AS FLOAT)),2)  AS DiemTB,
            SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                     THEN 1 ELSE 0 END)             AS TichCuc,
            SUM(CASE WHEN dg.NhanCamXuc = N'Tiêu cực'
                     THEN 1 ELSE 0 END)             AS TieuCuc,
            SUM(CASE WHEN dg.NhanCamXuc = N'Trung lập'
                     THEN 1 ELSE 0 END)             AS TrungLap
        FROM DanhGia dg
        INNER JOIN CoSoLuuTru cs ON cs.MaCoSo   = dg.MaCoSo
        INNER JOIN KhuVuc     kv ON kv.MaKhuVuc = cs.MaKhuVuc
        WHERE dg.LoaiKhach IS NOT NULL
        GROUP BY kv.TinhThanh, dg.LoaiKhach, dg.NgonNgu
        ORDER BY kv.TinhThanh, SoDanhGia DESC
        """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "02_khach_viet_vs_quocte.xlsx",
            index=False,
            sheet_name="Khách Việt vs Quốc Tế",
        )
        log.info(
            f"  ✅ Xuất: 02_khach_viet_vs_quocte.xlsx ({len(df)} dòng)"
        )
        return df

    # ── Report 03: Top điểm đến theo loại khách ──────────────
    def report_top_diem_den(self) -> dict:
        loai_khach_list = [
            "Khách Việt",       "Khách Hàn Quốc",
            "Khách Trung Quốc", "Khách Nhật Bản",
            "Khách Anh/Mỹ",     "Khách Pháp",
            "Khách Đức",        "Khách Nga",
            "Khách Thái Lan",   "Khách Malaysia",
        ]
        sql = """
        SELECT TOP 20
            kv.TinhThanh,
            kv.VungMien,
            COUNT(dg.MaDanhGia)                      AS SoDanhGia,
            ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB,
            ROUND(
                SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                         THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(dg.MaDanhGia), 0) * 100
            , 1)                                     AS TyLeTichCuc,
            COUNT(DISTINCT cs.MaCoSo)                AS SoKhachSan
            FROM DanhGia dg
            INNER JOIN CoSoLuuTru cs ON cs.MaCoSo   = dg.MaCoSo
            INNER JOIN KhuVuc     kv ON kv.MaKhuVuc = cs.MaKhuVuc
            WHERE dg.LoaiKhach = ?
            GROUP BY kv.TinhThanh, kv.VungMien
            ORDER BY SoDanhGia DESC, DiemTB DESC
            """
        results: dict = {}
        with pd.ExcelWriter(
                self.output_dir / "03_top_diem_den_theo_loai_khach.xlsx",
                engine="openpyxl",
        ) as writer:
            for loai in loai_khach_list:
                df = self._query(sql, (loai,))
                df.to_excel(
                    writer,
                    sheet_name=loai[:31],
                    index=False,
                )
                results[loai] = df
        log.info(
            "  ✅ Xuất: 03_top_diem_den_theo_loai_khach.xlsx"
        )
        return results

    # ── Report 04: Sentiment theo tỉnh ───────────────────────
    def report_sentiment_analysis(self) -> pd.DataFrame:
        sql = """
            SELECT
                kv.TinhThanh,
                kv.VungMien,
                cs.LoaiCoSo,
                dg.NhanCamXuc,
                dg.LoaiKhach,
                COUNT(*)                                 AS SoLuong,
                ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB,
                ROUND(
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
                        PARTITION BY kv.TinhThanh
                    )
                , 1)                                     AS TyLe_Pct
            FROM DanhGia dg
            INNER JOIN CoSoLuuTru cs ON cs.MaCoSo   = dg.MaCoSo
            INNER JOIN KhuVuc     kv ON kv.MaKhuVuc = cs.MaKhuVuc
            WHERE dg.NhanCamXuc IS NOT NULL
            GROUP BY
                kv.TinhThanh, kv.VungMien,
                cs.LoaiCoSo,  dg.NhanCamXuc,
                dg.LoaiKhach
            ORDER BY kv.TinhThanh, SoLuong DESC
            """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "04_sentiment_theo_tinh.xlsx",
            index=False,
            sheet_name="Sentiment",
        )
        log.info(
            f"  ✅ Xuất: 04_sentiment_theo_tinh.xlsx ({len(df)} dòng)"
        )
        return df

    # ── Report 05: Giá theo mùa ──────────────────────────────
    def report_gia_theo_mua(self) -> pd.DataFrame:
        sql = """
            SELECT
                kv.TinhThanh,
                kv.VungMien,
                lp.TenLoai                               AS LoaiPhong,
                MONTH(lg.NgayCheck)                      AS Thang,
                CASE
                    WHEN MONTH(lg.NgayCheck) IN (6,7,8)
                        THEN N'Hè'
                    WHEN MONTH(lg.NgayCheck) IN (12,1,2)
                        THEN N'Đông/Tết'
                    WHEN MONTH(lg.NgayCheck) IN (3,4,5)
                        THEN N'Xuân'
                    ELSE N'Thu'
                END                                      AS Mua,
                COUNT(*)                                 AS SoMau,
                ROUND(AVG(lg.GiaHienTai), 0)             AS GiaTB,
                ROUND(MIN(lg.GiaHienTai), 0)             AS GiaMin,
                ROUND(MAX(lg.GiaHienTai), 0)             AS GiaMax,
                ROUND(STDEV(lg.GiaHienTai), 0)           AS DoLechChuan,
                ROUND(AVG(lg.PhanTramGiam), 1)           AS GiamGiaTB_Pct
            FROM LichSuGia lg
            INNER JOIN LoaiPhong_Ve lp ON lp.MaLoai  = lg.MaLoai
            INNER JOIN CoSoLuuTru  cs ON cs.MaCoSo   = lp.MaCoSo
            INNER JOIN KhuVuc      kv ON kv.MaKhuVuc = cs.MaKhuVuc
            WHERE lg.GiaHienTai BETWEEN 100000 AND 50000000
            GROUP BY
                kv.TinhThanh, kv.VungMien, lp.TenLoai,
                MONTH(lg.NgayCheck),
                CASE
                    WHEN MONTH(lg.NgayCheck) IN (6,7,8)
                        THEN N'Hè'
                    WHEN MONTH(lg.NgayCheck) IN (12,1,2)
                        THEN N'Đông/Tết'
                    WHEN MONTH(lg.NgayCheck) IN (3,4,5)
                        THEN N'Xuân'
                    ELSE N'Thu'
                END
            ORDER BY kv.TinhThanh, Thang
            """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "05_gia_theo_mua.xlsx",
            index=False,
            sheet_name="Giá Theo Mùa",
        )
        log.info(
            f"  ✅ Xuất: 05_gia_theo_mua.xlsx ({len(df)} dòng)"
        )
        return df

    # ── Report 06: Phân tích theo nguồn ──────────────────────
    def report_theo_nguon(self) -> pd.DataFrame:
        sql = """
            SELECT
                dg.NguonDL                               AS NguonDuLieu,
                COUNT(*)                                 AS TongDanhGia,
                COUNT(DISTINCT dg.MaCoSo)                AS SoKhachSan,
                ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB,
                ROUND(AVG(LEN(dg.NoiDung)), 0)           AS DoDaiTB_KyTu,
                SUM(CASE WHEN dg.NgonNgu = 'vi'
                         THEN 1 ELSE 0 END)              AS TiengViet,
                SUM(CASE WHEN dg.NgonNgu = 'en'
                         THEN 1 ELSE 0 END)              AS TiengAnh,
                SUM(CASE WHEN dg.NgonNgu NOT IN ('vi','en')
                         THEN 1 ELSE 0 END)              AS NgonNguKhac,
                MIN(dg.NgayTao)                          AS NgayBatDau,
                MAX(dg.NgayTao)                          AS NgayKetThuc
            FROM DanhGia dg
            WHERE dg.NguonDL IS NOT NULL
            GROUP BY dg.NguonDL
            ORDER BY TongDanhGia DESC
            """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "06_phan_tich_theo_nguon.xlsx",
            index=False,
            sheet_name="Theo Nguồn",
        )
        log.info(
            f"  ✅ Xuất: 06_phan_tich_theo_nguon.xlsx ({len(df)} nguồn)"
        )
        return df

    # ── Report 07: Xu hướng theo thời gian ───────────────────
    def report_xu_huong_theo_thoi_gian(self) -> pd.DataFrame:
        sql = """
            SELECT
                kv.TinhThanh,
                YEAR(dg.NgayTao)                         AS Nam,
                MONTH(dg.NgayTao)                        AS Thang,
                COUNT(*)                                 AS SoDanhGia,
                ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB,
                SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                         THEN 1 ELSE 0 END)              AS TichCuc,
                SUM(CASE WHEN dg.NhanCamXuc = N'Tiêu cực'
                         THEN 1 ELSE 0 END)              AS TieuCuc
            FROM DanhGia dg
            INNER JOIN CoSoLuuTru cs ON cs.MaCoSo   = dg.MaCoSo
            INNER JOIN KhuVuc     kv ON kv.MaKhuVuc = cs.MaKhuVuc
            WHERE dg.NgayTao >= DATEADD(YEAR, -3, GETDATE())
            GROUP BY
                kv.TinhThanh,
                YEAR(dg.NgayTao),
                MONTH(dg.NgayTao)
            ORDER BY kv.TinhThanh, Nam, Thang
            """
        df = self._query(sql)
        df.to_excel(
            self.output_dir / "07_xu_huong_theo_thoi_gian.xlsx",
            index=False,
            sheet_name="Xu Hướng",
        )
        log.info(
            f"  ✅ Xuất: 07_xu_huong_theo_thoi_gian.xlsx ({len(df)} dòng)"
        )
        return df

    # ── Run all ───────────────────────────────────────────────
    def run_all(self) -> None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log.info("=" * 65)
        log.info(f"📊  BẮT ĐẦU XUẤT BÁO CÁO — {timestamp}")
        log.info("=" * 65)

        reports = [
            ("Báo cáo tổng hợp", self.report_tong_hop_cuoi),
            ("Tổng quan 63 tỉnh", self.report_tong_quan),
            ("Khách Việt vs Quốc Tế", self.report_khach_viet_vs_quocte),
            ("Top điểm đến theo loại khách", self.report_top_diem_den),
            ("Sentiment theo tỉnh", self.report_sentiment_analysis),
            ("Giá theo mùa", self.report_gia_theo_mua),
            ("Phân tích theo nguồn", self.report_theo_nguon),
            ("Xu hướng theo thời gian", self.report_xu_huong_theo_thoi_gian),
        ]

        success = 0
        failed = 0
        for name, func in reports:
            try:
                log.info(f"  📝  Đang xuất: {name}...")
                func()
                success += 1
            except Exception as e:
                log.error(f"  ❌  Lỗi khi xuất {name}: {e}")
                failed += 1

        log.info("=" * 65)
        log.info(
            f"🎉  Hoàn tất! "
            f"Thành công: {success} | Thất bại: {failed}"
        )
        log.info(
            f"📁  Báo cáo lưu tại: {self.output_dir.absolute()}"
        )
        log.info("=" * 65)