# Hasaki Raw Data Crawler

Professional data engineering crawler for Hasaki.vn product and review data with incremental snapshot strategy and automated GitHub Actions pipeline.

## 🎯 Features

### Core Capabilities
- **2-Phase Crawling Architecture**
  - Phase 1: Listing Discovery (weekly) - Khám phá tất cả products
  - Phase 2: Product & Review Updates (daily) - Cập nhật chi tiết

- **Incremental Snapshot**
  - Chỉ lưu data khi có thay đổi
  - Direct JSONB comparison trong PostgreSQL
  - Không cần Python-side hashing

- **Smart Review Pagination**
  - Tự động tính số pages từ `total` field
  - Phòng tránh API bug (lặp lại page cuối)
  - Crawl chính xác 100% review pages

- **Production-Ready**
  - Retry logic cho socket errors (3 attempts)
  - Parallel processing (15 product workers, 6 review workers)
  - Comprehensive metrics & logging
  - Database-driven product discovery

### Performance
- **Products**: ~23 products/second
- **Reviews**: ~11 pages/second (multi-page per product)
- **Duration**: 1-2 minutes cho 212 products + 750 review pages
- **Efficiency**: 46% storage savings (incremental snapshot)

---

## 📋 Table of Contents

1. [Architecture](#-architecture)
2. [Database Schema](#-database-schema)
3. [Installation](#-installation)
4. [Configuration](#-configuration)
5. [Usage](#-usage)
6. [GitHub Actions](#-github-actions)
7. [Monitoring](#-monitoring)
8. [Troubleshooting](#-troubleshooting)

---

## 🏗️ Architecture

### Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    HASAKI CRAWLER PIPELINE                    │
└─────────────────────────────────────────────────────────────┘

PHASE 1: LISTING DISCOVERY (Weekly - Thứ 2)
┌──────────────┐
│  Home API    │ → Get all categories
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Categories   │ → Filter leaf categories (no sub-categories)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Listing API  │ → Parallel crawl (20 workers)
│ (All Pages)  │ → Store product IDs + brand IDs → listing_api
└──────────────┘

PHASE 2: PRODUCT & REVIEW CRAWL (Daily)
┌──────────────┐
│  Database    │ → Query product IDs by brand_ids (0.6s)
│ (listing_api)│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Product API  │ → Parallel crawl (15 workers)
│              │ → Incremental: Only changed data → product_api
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Review API  │ → Parallel crawl (6 workers)
│ (Multi-page) │ → Smart pagination (prevent API bug)
│              │ → Full page JSON → review_api
└──────────────┘
```

### Data Flow

```
API → Python Client → Supabase Storage → PostgreSQL Triggers
                          ↓
                    Direct JSONB
                    Comparison
                          ↓
                  Insert / Skip (NULL)
```

---

## 💾 Database Schema

### Tables

**`crawl_sessions`**
- Track mỗi lần chạy crawler
- Fields: `session_id`, `api_type`, `started_at`, `finished_at`, `status`

**`home_api`**
- Snapshot của home page API (categories, banners, etc.)
- Trigger: Chỉ insert khi data thay đổi

**`listing_api`**
- Product IDs từ listing pages
- Fields: `product_id`, `brand_id`, `session_id`
- Purpose: Product discovery for Phase 2

**`product_api`**
- Chi tiết sản phẩm (title, price, rating, images, etc.)
- Trigger: Chỉ insert khi data thay đổi (incremental snapshot)
- Unique: `(product_id, data)` via trigger

**`review_api`**
- Full page JSON của reviews
- Fields: `product_id`, `pages` (page number), `data` (full JSON)
- Unique: `(product_id, pages)` + JSONB comparison via trigger

### Triggers

**Deduplication (Direct JSONB Comparison):**
```sql
-- Product: Check product_id + data
IF EXISTS (SELECT 1 WHERE product_id = NEW.product_id AND data = NEW.data)
    THEN RETURN NULL;

-- Review: Check product_id + pages + data  
IF EXISTS (SELECT 1 WHERE product_id = NEW.product_id AND pages = NEW.pages AND data = NEW.data)
    THEN RETURN NULL;
```

---

## 🔧 Installation

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ (Supabase)
- Git (for GitHub Actions)

### Local Setup

```bash
# 1. Clone repository
git clone https://github.com/YOUR_USERNAME/hasaki_raw.git
cd hasaki_raw

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc
venv\Scripts\activate     # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp env.example .env
# Edit .env với Supabase credentials
```

### Supabase Setup

```bash
# 1. Copy toàn bộ schema.sql
# 2. Vào Supabase SQL Editor
# 3. Paste và run toàn bộ file

# 4. Verify
SELECT * FROM raw.crawl_sessions LIMIT 1;
```

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```env
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-key
SUPABASE_SCHEMA=raw

# Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR

# API Endpoints (đã config trong env.example)
HOME_API=https://hasaki.vn/...
LISTING_API=https://hasaki.vn/...
PRODUCT_API=https://hasaki.vn/...
REVIEW_API=https://hasaki.vn/...
```

### Target Brands (`brands.txt`)

```
105    # CeraVe
1927   # Cocoon
# Add more brand IDs here
```

**Tìm brand IDs:**
```bash
python find_brands.py
```

---

## 🚀 Usage

### Local Development

```bash
# Phase 1: Listing Crawl (chạy 1 lần/tuần)
python crawl_listings.py

# Phase 2: Product & Review Crawl (chạy hàng ngày)
python crawler.py
```

### Manual Testing

```bash
# Test specific product
python
>>> from api_client import HasakiAPIClient
>>> client = HasakiAPIClient()
>>> data, meta = client.get_product_detail(84643)
>>> reviews = client.get_product_reviews(84643)
```

---

## 🤖 GitHub Actions

### Setup

1. **Push code lên GitHub**
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/hasaki_raw.git
   git push -u origin main
   ```

2. **Add Secrets**
   - Vào: Settings → Secrets and variables → Actions
   - Add: `SUPABASE_URL`, `SUPABASE_KEY`

3. **Done!** Workflow tự động chạy

### Schedule

- **Weekly**: Thứ 2, 1:00 AM UTC (8:00 AM VN) - Listing Crawl
- **Daily**: Mỗi ngày, 2:00 AM UTC (9:00 AM VN) - Product Crawl

### Manual Trigger

1. **Actions** → **Hasaki Crawler**
2. **Run workflow**
3. Chọn: `both` / `listing` / `product`

**Chi tiết:** Xem [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)

---

## 📊 Monitoring

### Check Crawl Status

```sql
-- Session mới nhất
SELECT * FROM raw.crawl_sessions 
ORDER BY started_at DESC LIMIT 1;

-- Stats per session
SELECT 
    session_id,
    started_at,
    finished_at,
    status,
    total_items,
    skipped_items,
    EXTRACT(EPOCH FROM (finished_at - started_at)) as duration_seconds
FROM raw.crawl_sessions
ORDER BY started_at DESC;
```

### Check Data Quality

```sql
-- Products per brand
SELECT 
    LEFT(product_id, 2) as brand_prefix,
    COUNT(DISTINCT product_id) as products
FROM raw.product_api
GROUP BY brand_prefix;

-- Review coverage
SELECT 
    COUNT(DISTINCT pa.product_id) as products_with_details,
    COUNT(DISTINCT ra.product_id) as products_with_reviews,
    ROUND(COUNT(DISTINCT ra.product_id)::numeric / 
          COUNT(DISTINCT pa.product_id) * 100, 2) as review_coverage_pct
FROM raw.product_api pa
LEFT JOIN raw.review_api ra ON pa.product_id = ra.product_id;

-- Top reviewed products
SELECT 
    product_id,
    COUNT(*) as review_pages,
    MAX(pages) as last_page,
    MIN(created_at) as first_crawl,
    MAX(created_at) as last_crawl
FROM raw.review_api
GROUP BY product_id
ORDER BY review_pages DESC
LIMIT 10;
```

---

## 🔍 Troubleshooting

### Common Issues

#### ❌ "No products found in database"
```bash
# Chạy listing crawl trước
python crawl_listings.py
```

#### ❌ "Permission denied for schema raw"
```sql
-- Supabase SQL Editor
GRANT ALL ON SCHEMA raw TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO anon, authenticated, service_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA raw TO anon, authenticated, service_role;
```

#### ❌ "WinError 10035" (Socket errors)
```python
# Giảm workers trong crawler.py
MAX_REVIEW_WORKERS = 4  # Giảm từ 6 xuống 4
```

#### ❌ Products bị duplicate
```sql
-- Check trigger
SELECT * FROM pg_trigger WHERE tgname = 'trigger_product_change_detection';

-- Recreate trigger (copy từ schema.sql)
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python crawler.py

# Hoặc edit .env
LOG_LEVEL=DEBUG
```

---

## 📚 Documentation

- [FINAL_FIXES.md](FINAL_FIXES.md) - Chi tiết tất cả fixes
- [API_BUG_FIX.md](API_BUG_FIX.md) - Hasaki API bug & solution
- [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md) - GitHub Actions guide

---

## 📈 Performance Benchmarks

**System:** GitHub Actions (ubuntu-latest, 2 CPU cores)

| Phase | Items | Duration | Throughput | Workers |
|-------|-------|----------|------------|---------|
| Listing | ~3000 products | 10-15 min | 3-5 products/s | 20 parallel |
| Product | 212 products | 9-10s | 22-23 products/s | 15 parallel |
| Review | 750 pages | 60-70s | 10-12 pages/s | 6 parallel |

**Storage Efficiency:** 46% duplicate rate (incremental snapshot working!)

---

## 🎉 Credits

Developed by [Your Name]

**Tech Stack:**
- Python 3.10
- Supabase (PostgreSQL 14)
- GitHub Actions
- requests, python-dotenv, supabase-py

---

## 📝 License

[Your License] (MIT recommended)

---

## 🤝 Contributing

Contributions welcome! Please:
1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Open Pull Request

---

**Happy Crawling! 🚀**
