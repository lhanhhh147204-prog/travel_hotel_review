-- ══════════════════════════════════════════════════════════════
-- DATABASE SCHEMA — HotelReviews Vietnam
-- SQL Server 2019+
-- ══════════════════════════════════════════════════════════════

USE master;
GO

IF NOT EXISTS (
    SELECT name FROM sys.databases WHERE name = N'BIG_DATA'
)
    CREATE DATABASE BIG_DATA
    COLLATE Vietnamese_CI_AS;
GO

USE BIG_DATA;
GO

-- ── 1. KhuVuc ─────────────────────────────────────────────────
IF OBJECT_ID('KhuVuc', 'U') IS NULL
CREATE TABLE KhuVuc (
    MaKhuVuc    INT           IDENTITY(1,1) PRIMARY KEY,
    TinhThanh   NVARCHAR(100) NOT NULL,
    VungMien    NVARCHAR(50)  NOT NULL
        CHECK (VungMien IN (
            N'Miền Bắc', N'Miền Trung', N'Miền Nam'
        )),
    Tier        NVARCHAR(10)  NULL
        CHECK (Tier IN ('tier_1', 'tier_2', 'tier_3')),
    CreatedAt   DATETIME      DEFAULT GETDATE(),
    UpdatedAt   DATETIME      DEFAULT GETDATE(),

    CONSTRAINT UQ_KhuVuc_TinhThanh UNIQUE (TinhThanh),
    INDEX IX_KhuVuc_VungMien (VungMien),
    INDEX IX_KhuVuc_Tier     (Tier),
);
GO

-- ── 2. CoSoLuuTru ─────────────────────────────────────────────
IF OBJECT_ID('CoSoLuuTru', 'U') IS NULL
CREATE TABLE CoSoLuuTru (
    MaCoSo          INT            IDENTITY(1,1) PRIMARY KEY,
    MaKhuVuc        INT            NOT NULL
        REFERENCES KhuVuc(MaKhuVuc),
    TenCoSo         NVARCHAR(500)  NOT NULL,
    DiaChiChiTiet   NVARCHAR(500)  NULL,
    SoSao           FLOAT          NULL
        CHECK (SoSao BETWEEN 0 AND 5),
    LoaiCoSo        NVARCHAR(50)   NOT NULL DEFAULT N'Khách Sạn'
        CHECK (LoaiCoSo IN (
            N'Khách Sạn', N'Resort', N'Homestay',
            N'Nhà Nghỉ',  N'Căn Hộ', N'Villa',
            N'Bungalow',  N'Hostel'
        )),
    DiemTrungBinh   FLOAT          NULL,
    TongSoReview    INT            DEFAULT 0,
    MoTa            NVARCHAR(50)   NULL,   -- URL hash (dedup key)
    NguonDuLieu     NVARCHAR(100)  NULL,
    UrlGoc          NVARCHAR(1000) NULL,
    CreatedAt       DATETIME       DEFAULT GETDATE(),
    UpdatedAt       DATETIME       DEFAULT GETDATE(),

    INDEX IX_CoSo_KhuVuc    (MaKhuVuc),
    INDEX IX_CoSo_LoaiCoSo  (LoaiCoSo),
    INDEX IX_CoSo_DiemTB    (DiemTrungBinh DESC),
    INDEX IX_CoSo_MoTa      (MoTa),
    INDEX IX_CoSo_NguonDL   (NguonDuLieu),
);
GO

-- ── 3. LoaiPhong_Ve ───────────────────────────────────────────
IF OBJECT_ID('LoaiPhong_Ve', 'U') IS NULL
CREATE TABLE LoaiPhong_Ve (
    MaLoai      INT           IDENTITY(1,1) PRIMARY KEY,
    MaCoSo      INT           NOT NULL
        REFERENCES CoSoLuuTru(MaCoSo),
    TenLoai     NVARCHAR(200) NOT NULL,
    MoTa        NVARCHAR(500) NULL,
    CreatedAt   DATETIME      DEFAULT GETDATE(),

    CONSTRAINT UQ_LoaiPhong_CoSo_Ten
        UNIQUE (MaCoSo, TenLoai),
    INDEX IX_LoaiPhong_CoSo (MaCoSo),
);
GO

-- ── 4. LichSuGia ──────────────────────────────────────────────
IF OBJECT_ID('LichSuGia', 'U') IS NULL
CREATE TABLE LichSuGia (
    MaGia           INT     IDENTITY(1,1) PRIMARY KEY,
    MaLoai          INT     NOT NULL
        REFERENCES LoaiPhong_Ve(MaLoai),
    GiaHienTai      FLOAT   NOT NULL
        CHECK (GiaHienTai > 0),
    GiaGoc          FLOAT   NULL,
    PhanTramGiam    FLOAT   NULL
        CHECK (PhanTramGiam BETWEEN 0 AND 100),
    ConPhong        BIT     DEFAULT 1,
    NgayCheck       DATE    DEFAULT CAST(GETDATE() AS DATE),
    CreatedAt       DATETIME DEFAULT GETDATE(),

    INDEX IX_LichSuGia_MaLoai   (MaLoai),
    INDEX IX_LichSuGia_NgayCheck (NgayCheck DESC),
    INDEX IX_LichSuGia_Gia      (GiaHienTai),
);
GO

-- ── 5. DanhGia ────────────────────────────────────────────────
IF OBJECT_ID('DanhGia', 'U') IS NULL
CREATE TABLE DanhGia (
    MaDanhGia       INT            IDENTITY(1,1) PRIMARY KEY,
    MaCoSo          INT            NOT NULL
        REFERENCES CoSoLuuTru(MaCoSo),
    TenKhachHang    NVARCHAR(200)  NOT NULL DEFAULT N'Ẩn danh',
    QuocTich        NVARCHAR(100)  NULL,
    NgonNgu         NVARCHAR(10)   NOT NULL DEFAULT 'vi',
    LoaiKhach       NVARCHAR(50)   NULL,
    SoDiem          FLOAT          NULL
        CHECK (SoDiem BETWEEN 0 AND 10),
    NoiDungBinhLuan NVARCHAR(4000) NULL,
    TieuDe          NVARCHAR(500)  NULL,
    NgayDanhGia     DATE           NULL,
    LoaiPhongDaO    NVARCHAR(200)  NULL,
    NhanCamXuc      NVARCHAR(20)   NULL
        CHECK (NhanCamXuc IN (
            N'Tích cực', N'Tiêu cực', N'Trung lập'
        )),
    ReviewHash      CHAR(32)       NULL,
    NguonDuLieu     NVARCHAR(100)  NULL,
    CreatedAt       DATETIME       DEFAULT GETDATE(),

    CONSTRAINT UQ_DanhGia_Hash
        UNIQUE (ReviewHash),
    INDEX IX_DanhGia_CoSo       (MaCoSo),
    INDEX IX_DanhGia_NgonNgu    (NgonNgu),
    INDEX IX_DanhGia_LoaiKhach  (LoaiKhach),
    INDEX IX_DanhGia_CamXuc     (NhanCamXuc),
    INDEX IX_DanhGia_NgayDG     (NgayDanhGia DESC),
    INDEX IX_DanhGia_Diem       (SoDiem DESC),
    INDEX IX_DanhGia_NguonDL    (NguonDuLieu),
);
GO

-- ── 6. Seed KhuVuc data ───────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM KhuVuc)
BEGIN
    INSERT INTO KhuVuc (TinhThanh, VungMien, Tier) VALUES
    -- Tier 1
    (N'Hà Nội',           N'Miền Bắc',   'tier_1'),
    (N'Hồ Chí Minh',      N'Miền Nam',   'tier_1'),
    (N'Đà Nẵng',          N'Miền Trung', 'tier_1'),
    (N'Khánh Hòa',        N'Miền Trung', 'tier_1'),
    (N'Kiên Giang',       N'Miền Nam',   'tier_1'),
    (N'Lâm Đồng',         N'Miền Trung', 'tier_1'),
    (N'Quảng Nam',        N'Miền Trung', 'tier_1'),
    (N'Thừa Thiên Huế',   N'Miền Trung', 'tier_1'),
    (N'Quảng Ninh',       N'Miền Bắc',   'tier_1'),
    (N'Lào Cai',          N'Miền Bắc',   'tier_1'),
    -- Tier 2
    (N'Bình Thuận',       N'Miền Trung', 'tier_2'),
    (N'Bà Rịa-Vũng Tàu', N'Miền Nam',   'tier_2'),
    (N'Ninh Bình',        N'Miền Bắc',   'tier_2'),
    (N'Hải Phòng',        N'Miền Bắc',   'tier_2'),
    (N'Bình Định',        N'Miền Trung', 'tier_2'),
    (N'Phú Yên',          N'Miền Trung', 'tier_2'),
    (N'Đắk Lắk',         N'Miền Trung', 'tier_2'),
    (N'Hà Giang',         N'Miền Bắc',   'tier_2'),
    (N'Điện Biên',        N'Miền Bắc',   'tier_2'),
    (N'Sơn La',           N'Miền Bắc',   'tier_2'),
    (N'Hòa Bình',         N'Miền Bắc',   'tier_2'),
    (N'Thanh Hóa',        N'Miền Trung', 'tier_2'),
    (N'Nghệ An',          N'Miền Trung', 'tier_2'),
    (N'Quảng Bình',       N'Miền Trung', 'tier_2'),
    -- Tier 3
    (N'An Giang',         N'Miền Nam',   'tier_3'),
    (N'Bạc Liêu',         N'Miền Nam',   'tier_3'),
    (N'Bắc Giang',        N'Miền Bắc',   'tier_3'),
        -- Tier 3 (tiếp)
    (N'Bắc Kạn',          N'Miền Bắc',   'tier_3'),
    (N'Bắc Ninh',         N'Miền Bắc',   'tier_3'),
    (N'Bến Tre',          N'Miền Nam',   'tier_3'),
    (N'Bình Dương',       N'Miền Nam',   'tier_3'),
    (N'Bình Phước',       N'Miền Nam',   'tier_3'),
    (N'Cà Mau',           N'Miền Nam',   'tier_3'),
    (N'Cần Thơ',          N'Miền Nam',   'tier_3'),
    (N'Cao Bằng',         N'Miền Bắc',   'tier_3'),
    (N'Đắk Nông',         N'Miền Trung', 'tier_3'),
    (N'Đồng Nai',         N'Miền Nam',   'tier_3'),
    (N'Đồng Tháp',        N'Miền Nam',   'tier_3'),
    (N'Gia Lai',          N'Miền Trung', 'tier_3'),
    (N'Hà Nam',           N'Miền Bắc',   'tier_3'),
    (N'Hà Tĩnh',          N'Miền Trung', 'tier_3'),
    (N'Hải Dương',        N'Miền Bắc',   'tier_3'),
    (N'Hậu Giang',        N'Miền Nam',   'tier_3'),
    (N'Hưng Yên',         N'Miền Bắc',   'tier_3'),
    (N'Kon Tum',          N'Miền Trung', 'tier_3'),
    (N'Lai Châu',         N'Miền Bắc',   'tier_3'),
    (N'Lạng Sơn',         N'Miền Bắc',   'tier_3'),
    (N'Long An',          N'Miền Nam',   'tier_3'),
    (N'Nam Định',         N'Miền Bắc',   'tier_3'),
    (N'Ninh Thuận',       N'Miền Trung', 'tier_3'),
    (N'Phú Thọ',          N'Miền Bắc',   'tier_3'),
    (N'Quảng Ngãi',       N'Miền Trung', 'tier_3'),
    (N'Quảng Trị',        N'Miền Trung', 'tier_3'),
    (N'Sóc Trăng',        N'Miền Nam',   'tier_3'),
    (N'Tây Ninh',         N'Miền Nam',   'tier_3'),
    (N'Thái Bình',        N'Miền Bắc',   'tier_3'),
    (N'Thái Nguyên',      N'Miền Bắc',   'tier_3'),
    (N'Tiền Giang',       N'Miền Nam',   'tier_3'),
    (N'Trà Vinh',         N'Miền Nam',   'tier_3'),
    (N'Tuyên Quang',      N'Miền Bắc',   'tier_3'),
    (N'Vĩnh Long',        N'Miền Nam',   'tier_3'),
    (N'Vĩnh Phúc',        N'Miền Bắc',   'tier_3'),
    (N'Yên Bái',          N'Miền Bắc',   'tier_3');
END
GO

-- ── 7. Views ──────────────────────────────────────────────────

-- View: Tổng hợp theo tỉnh
CREATE OR ALTER VIEW vw_TongHop_Tinh AS
SELECT
    kv.MaKhuVuc,
    kv.TinhThanh,
    kv.VungMien,
    kv.Tier,
    COUNT(DISTINCT cs.MaCoSo)                AS TongCoSo,
    COUNT(DISTINCT CASE
        WHEN cs.LoaiCoSo = N'Khách Sạn'
        THEN cs.MaCoSo END)                  AS SoKhachSan,
    COUNT(DISTINCT CASE
        WHEN cs.LoaiCoSo = N'Resort'
        THEN cs.MaCoSo END)                  AS SoResort,
    COUNT(DISTINCT CASE
        WHEN cs.LoaiCoSo = N'Homestay'
        THEN cs.MaCoSo END)                  AS SoHomestay,
    COUNT(dg.MaDanhGia)                      AS TongDanhGia,
    SUM(CASE WHEN dg.NgonNgu = 'vi'
             THEN 1 ELSE 0 END)              AS KhachViet,
    SUM(CASE WHEN dg.NgonNgu != 'vi'
             THEN 1 ELSE 0 END)              AS KhachQuocTe,
    ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTrungBinh,
    ROUND(
        SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                 THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(dg.MaDanhGia), 0) * 100
    , 1)                                     AS TyLeTichCuc_Pct,
    ROUND(AVG(lg.GiaHienTai), 0)             AS GiaTrungBinh_VND,
    ROUND(MIN(lg.GiaHienTai), 0)             AS GiaThapNhat_VND,
    ROUND(MAX(lg.GiaHienTai), 0)             AS GiaCaoNhat_VND
FROM KhuVuc kv
LEFT JOIN CoSoLuuTru   cs ON cs.MaKhuVuc = kv.MaKhuVuc
LEFT JOIN DanhGia      dg ON dg.MaCoSo   = cs.MaCoSo
LEFT JOIN LoaiPhong_Ve lp ON lp.MaCoSo   = cs.MaCoSo
LEFT JOIN LichSuGia    lg ON lg.MaLoai   = lp.MaLoai
GROUP BY
    kv.MaKhuVuc, kv.TinhThanh,
    kv.VungMien,  kv.Tier;
GO

-- View: Top khách sạn theo điểm
CREATE OR ALTER VIEW vw_TopKhachSan AS
SELECT TOP 1000
    cs.MaCoSo,
    cs.TenCoSo,
    cs.LoaiCoSo,
    cs.SoSao,
    kv.TinhThanh,
    kv.VungMien,
    cs.DiemTrungBinh,
    cs.TongSoReview,
    ROUND(AVG(lg.GiaHienTai), 0)             AS GiaTB_VND,
    ROUND(
        SUM(CASE WHEN dg.NhanCamXuc = N'Tích cực'
                 THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(dg.MaDanhGia), 0) * 100
    , 1)                                     AS TyLeTichCuc_Pct,
    SUM(CASE WHEN dg.NgonNgu = 'vi'
             THEN 1 ELSE 0 END)              AS KhachViet,
    SUM(CASE WHEN dg.NgonNgu != 'vi'
             THEN 1 ELSE 0 END)              AS KhachQuocTe,
    cs.NguonDuLieu,
    cs.UrlGoc
FROM CoSoLuuTru   cs
INNER JOIN KhuVuc      kv ON kv.MaKhuVuc = cs.MaKhuVuc
LEFT  JOIN DanhGia     dg ON dg.MaCoSo   = cs.MaCoSo
LEFT  JOIN LoaiPhong_Ve lp ON lp.MaCoSo  = cs.MaCoSo
LEFT  JOIN LichSuGia   lg ON lg.MaLoai   = lp.MaLoai
WHERE cs.DiemTrungBinh IS NOT NULL
GROUP BY
    cs.MaCoSo,    cs.TenCoSo,
    cs.LoaiCoSo,  cs.SoSao,
    kv.TinhThanh, kv.VungMien,
    cs.DiemTrungBinh, cs.TongSoReview,
    cs.NguonDuLieu,   cs.UrlGoc
ORDER BY cs.DiemTrungBinh DESC, cs.TongSoReview DESC;
GO

-- View: Phân tích sentiment theo tháng
CREATE OR ALTER VIEW vw_Sentiment_TheoThang AS
SELECT
    kv.TinhThanh,
    kv.VungMien,
    YEAR(dg.NgayDanhGia)                     AS Nam,
    MONTH(dg.NgayDanhGia)                    AS Thang,
    dg.NhanCamXuc,
    dg.NgonNgu,
    COUNT(*)                                 AS SoLuong,
    ROUND(AVG(CAST(dg.SoDiem AS FLOAT)), 2)  AS DiemTB
FROM DanhGia dg
INNER JOIN CoSoLuuTru cs ON cs.MaCoSo   = dg.MaCoSo
INNER JOIN KhuVuc     kv ON kv.MaKhuVuc = cs.MaKhuVuc
WHERE
    dg.NgayDanhGia IS NOT NULL
    AND dg.NhanCamXuc IS NOT NULL
GROUP BY
    kv.TinhThanh,        kv.VungMien,
    YEAR(dg.NgayDanhGia), MONTH(dg.NgayDanhGia),
    dg.NhanCamXuc,        dg.NgonNgu;
GO

-- ── 8. Stored Procedures ──────────────────────────────────────

-- SP: Cập nhật điểm trung bình hàng loạt
CREATE OR ALTER PROCEDURE sp_CapNhat_DiemTrungBinh
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE cs
    SET
        DiemTrungBinh = sub.DiemTB,
        TongSoReview  = sub.TongReview,
        UpdatedAt     = GETDATE()
    FROM CoSoLuuTru cs
    INNER JOIN (
        SELECT
            MaCoSo,
            ROUND(AVG(CAST(SoDiem AS FLOAT)), 2) AS DiemTB,
            COUNT(*)                              AS TongReview
        FROM DanhGia
        WHERE SoDiem > 0
        GROUP BY MaCoSo
    ) sub ON sub.MaCoSo = cs.MaCoSo;

    PRINT CONCAT(
        N'✅ Đã cập nhật điểm TB cho ',
        @@ROWCOUNT, N' cơ sở lưu trú'
    );
END;
GO

-- SP: Thống kê nhanh
CREATE OR ALTER PROCEDURE sp_ThongKe_Nhanh
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        N'Tổng cơ sở lưu trú' AS ChiTieu,
        CAST(COUNT(*) AS NVARCHAR) AS GiaTri
    FROM CoSoLuuTru
    UNION ALL
    SELECT N'Tổng đánh giá',
        CAST(COUNT(*) AS NVARCHAR)
    FROM DanhGia
    UNION ALL
    SELECT N'Tổng tỉnh thành',
        CAST(COUNT(*) AS NVARCHAR)
    FROM KhuVuc
    UNION ALL
    SELECT N'Khách Việt',
        CAST(SUM(CASE WHEN NgonNgu = 'vi'
                      THEN 1 ELSE 0 END) AS NVARCHAR)
    FROM DanhGia
    UNION ALL
    SELECT N'Khách Quốc Tế',
        CAST(SUM(CASE WHEN NgonNgu != 'vi'
                      THEN 1 ELSE 0 END) AS NVARCHAR)
    FROM DanhGia
    UNION ALL
    SELECT N'Điểm TB toàn quốc',
        CAST(ROUND(AVG(CAST(SoDiem AS FLOAT)), 2) AS NVARCHAR)
    FROM DanhGia
    WHERE SoDiem > 0
    UNION ALL
    SELECT N'Tỷ lệ tích cực %',
        CAST(ROUND(
            SUM(CASE WHEN NhanCamXuc = N'Tích cực'
                     THEN 1.0 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 1) AS NVARCHAR)
    FROM DanhGia
    WHERE NhanCamXuc IS NOT NULL;
END;
GO
select * from CoSoLuuTru
select * from DanhGia
select * from KhuVuc
select * from LichSuGia
select * from LoaiPhong_Ve