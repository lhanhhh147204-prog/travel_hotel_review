# 🏨 Hotel Review Scraper — Vietnam

## Cài đặt

```bash
# 1. Clone repo
git clone https://github.com/your-repo/hotel-scraper.git
cd hotel-scraper

# 2. Tạo virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# 3. Cài dependencies
pip install -r requirements.txt

# 4. Cài Playwright browsers
playwright install chromium

# 5. Cấu hình .env
cp .env.example .env
# Chỉnh sửa DB_SERVER, DB_NAME trong .env

# 6. Tạo database
sqlcmd -S localhost -i database.sql
```

## Sử dụng

```bash
# Ước tính thời gian chạy
python main.py estimate 3

# Chạy toàn bộ pipeline (tất cả tỉnh)
python main.py scrape all 5 3

# Chạy theo tier
python main.py scrape tier_1 5 5
python main.py scrape tier_2 3 3
python main.py scrape tier_3 2 2

# Chỉ thu thập URLs (Stage 1)
python main.py stage1 tier_1 5

# Chỉ scrape chi tiết (Stage 2)
python main.py stage2 hotel_urls_collected.txt 3

# Xuất báo cáo Excel
python main.py analyze
```

## Cấu trúc dữ liệu
