# Makefile — Hotel Review Scraper

.PHONY: install setup test lint format clean run-estimate \
        run-all run-tier1 run-tier2 run-tier3 \
        run-stage1 run-stage2 analyze help

# ── Cài đặt ───────────────────────────────────────────────────
install:
	pip install -r requirements.txt
	playwright install chromium

setup: install
	cp -n .env.example .env || true
	mkdir -p logs reports cookies
	@echo "✅  Setup hoàn tất. Chỉnh sửa .env trước khi chạy."

# ── Database ──────────────────────────────────────────────────
db-init:
	sqlcmd -S localhost -i database.sql
	@echo "✅  Database đã được khởi tạo."

db-stats:
	sqlcmd -S localhost -d BIG_DATA \
		-Q "EXEC sp_ThongKe_Nhanh"

db-update-scores:
	sqlcmd -S localhost -d BIG_DATA \
		-Q "EXEC sp_CapNhat_DiemTrungBinh"

# ── Test ──────────────────────────────────────────────────────
test:
	pytest tests/ -v \
		--tb=short \
		--cov=. \
		--cov-report=term-missing \
		--cov-report=html:reports/coverage \
		-x

test-fast:
	pytest tests/ -v --tb=short -x -q \
		--ignore=tests/test_scrapers.py

test-helpers:
	pytest tests/test_helpers.py -v

test-sentiment:
	pytest tests/test_sentiment.py -v

test-language:
	pytest tests/test_language_detect.py -v

test-repository:
	pytest tests/test_repository.py -v

test-pipeline:
	pytest tests/test_pipeline.py -v

test-url-gen:
	pytest tests/test_url_gen.py -v

test-scrapers:
	pytest tests/test_scrapers.py -v

# ── Code quality ──────────────────────────────────────────────
lint:
	flake8 . \
		--max-line-length=100 \
		--exclude=venv,.git,__pycache__ \
		--ignore=E501,W503

format:
	black . \
		--line-length=100 \
		--exclude="/(venv|\.git|__pycache__)/"
	isort . \
		--profile=black \
		--line-length=100

type-check:
	mypy . \
		--ignore-missing-imports \
		--exclude venv

# ── Chạy scraper ──────────────────────────────────────────────
run-estimate:
	python main.py estimate 3

run-all:
	python main.py scrape all 5 3

run-tier1:
	python main.py scrape tier_1 5 5

run-tier2:
	python main.py scrape tier_2 3 3

run-tier3:
	python main.py scrape tier_3 2 2

run-stage1:
	python main.py stage1 tier_1 5

run-stage2:
	python main.py stage2 hotel_urls_collected.txt 3

# ── Phân tích ─────────────────────────────────────────────────
analyze:
	python main.py analyze

# ── Dọn dẹp ──────────────────────────────────────────────────
clean:
	find . -type f -name "*.pyc"    -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov"  -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	@echo "✅  Đã dọn dẹp cache."

clean-state:
	rm -f scrape_state.json
	@echo "✅  Đã xóa scrape state."

clean-cookies:
	rm -f cookies/*.json
	@echo "✅  Đã xóa cookies."

clean-all: clean clean-state clean-cookies
	@echo "✅  Đã dọn dẹp tất cả."

# ── Help ──────────────────────────────────────────────────────
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════╗"
	@echo "║     HOTEL SCRAPER — MAKEFILE COMMANDS           ║"
	@echo "╠══════════════════════════════════════════════════╣"
	@echo "║  make install       Cài đặt dependencies        ║"
	@echo "║  make setup         Cài đặt + tạo thư mục       ║"
	@echo "║  make db-init       Khởi tạo database           ║"
	@echo "║  make db-stats      Xem thống kê DB             ║"
	@echo "║  make test          Chạy tất cả tests           ║"
	@echo "║  make test-fast     Chạy tests nhanh            ║"
	@echo "║  make lint          Kiểm tra code style         ║"
	@echo "║  make format        Format code                 ║"
	@echo "║  make run-estimate  Ước tính thời gian          ║"
	@echo "║  make run-all       Chạy toàn bộ pipeline       ║"
	@echo "║  make run-tier1     Chạy Tier 1                 ║"
	@echo "║  make run-tier2     Chạy Tier 2                 ║"
	@echo "║  make run-tier3     Chạy Tier 3                 ║"
	@echo "║  make run-stage1    Chỉ thu thập URLs           ║"
	@echo "║  make run-stage2    Chỉ scrape chi tiết         ║"
	@echo "║  make analyze       Xuất báo cáo Excel          ║"
	@echo "║  make clean         Dọn dẹp cache               ║"
	@echo "╚══════════════════════════════════════════════════╝"
	@echo ""