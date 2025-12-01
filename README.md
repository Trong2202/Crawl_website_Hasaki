# Hasaki Raw Data Crawler

Data pipeline tự động cho Hasaki.vn với incremental snapshot và smart review pagination.

## 🎯 Tính Năng

- **2-Phase Crawling**: Listing (tuần) + Product & Review (ngày)
- **Incremental Snapshot**: Chỉ lưu data thay đổi (JSONB comparison)
- **Smart Pagination**: Tự động detect kết thúc reviews (tránh API bug)
- **Production-Ready**: Retry logic, parallel processing, GitHub Actions

## 🏗️ Architecture

```
Weekly:  Home → Categories → Listing (20 workers) → listing_api
Daily:   Database → Product (10 workers) → product_api
                 ↓ Review (20 workers)  → review_api
```

**Workers Allocation:**
- Review: 20 (nặng nhất - multi-page per product)
- Listing: 20 (nặng - khám phá toàn bộ)
- Product: 10 (nhẹ nhất - 1 request per product)

## ⚡ Performance

- **Products**: ~20-25 products/s
- **Reviews**: ~15-20 pages/s
- **Duration**: 1-2 phút (212 products, 750+ review pages)


## 🤖 GitHub Actions

### Setup

1. Push code lên GitHub
2. Settings → Secrets → Add `SUPABASE_URL`, `SUPABASE_KEY`
3. Done! Tự động chạy:
   - **Thứ 2, 1:00 AM UTC**: Listing crawl
   - **Mỗi ngày, 2:00 AM UTC**: Product + Review crawl


## 💾 Database Schema

| Table | Purpose | Unique Constraint |
|-------|---------|-------------------|
| `crawl_sessions` | Track mỗi lần chạy | - |
| `home_api` | Home page snapshot | `data` (trigger) |
| `listing_api` | Product IDs | `(product_id, brand_id, session_id)` |
| `product_api` | Chi tiết sản phẩm | `(product_id, data)` (trigger) |
| `review_api` | Review pages | `(product_id, pages, data)` (trigger) |
cd 'c:\Users\ttron\Documents\A_Project\hasaki_raw'
Get-ChildItem -Recurse -File | Select-Object -ExpandProperty FullName
**Deduplication:** Direct JSONB comparison trong PostgreSQL triggers (không hash Python-side)


```sql
-- Session mới nhất
SELECT * FROM raw.crawl_sessions ORDER BY started_at DESC LIMIT 1;

-- Review coverage
SELECT 
    COUNT(DISTINCT pa.product_id) as products,
    COUNT(DISTINCT ra.product_id) as with_reviews,
    ROUND(100.0 * COUNT(DISTINCT ra.product_id) / COUNT(DISTINCT pa.product_id), 1) as coverage_pct
FROM raw.product_api pa
LEFT JOIN raw.review_api ra ON pa.product_id = ra.product_id;

-- Top reviewed
SELECT product_id, COUNT(*) as pages, MAX(pages) as last_page
FROM raw.review_api
GROUP BY product_id
ORDER BY pages DESC
LIMIT 10;
```

## 🔧 Configuration

### Environment (`.env`)

```env
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=your-key
SUPABASE_SCHEMA=raw

```

### Target Brands (`brands.txt`)

```
105    # CeraVe
1927   # Cocoon
```

Tìm thêm brands: `python find_brands.py`

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| "No products found" | `python crawl_listings.py` trước |
| "Permission denied" | Grant quyền trong schema.sql (cuối file) |
| Socket errors | Giảm `MAX_REVIEW_WORKERS` trong `crawler.py` |
| Duplicates | Check triggers trong `schema.sql` |

Debug mode: `export LOG_LEVEL=DEBUG`

## 📁 Project Structure

```
hasaki_raw/
├── crawler.py              # Main: Product & Review crawl
├── crawl_listings.py       # Phase 1: Listing crawl
├── api_client.py           # Hasaki API wrapper
├── supabase_client.py      # Database operations
├── config.py               # Configuration
├── logger.py               # Logging setup
├── schema.sql              # PostgreSQL schema
├── brands.txt              # Target brand IDs
├── requirements.txt        # Dependencies
└── .github/workflows/
    └── hasaki-crawler.yml  # GitHub Actions
```

