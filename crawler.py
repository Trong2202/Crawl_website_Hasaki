"""
Hasaki Crawler - Professional Data Engineering Edition

ARCHITECTURE:
- 2-Phase Crawling: Listing discovery (weekly) + Product/Review updates (daily)
- Database-driven: Read product IDs from listing_api table
- Incremental: Only store when data changes (direct JSONB comparison)
- Parallel: ThreadPoolExecutor with optimized worker counts
- Resilient: Error handling, progress tracking, comprehensive metrics

PERFORMANCE:
- Products: 8 workers (lightweight API calls)
- Reviews: 16 workers (multi-page pagination, avg 3.5 pages/product)
- Time: ~1-2 minutes for 200 products (depending on changes)
- Optimized for review-heavy workload (700+ pages from 200 products)

METRICS:
- Tracks both crawled (API calls) and inserted (new data) separately
- Review pages: Each product can have 1-N pages (paginated)
- Deduplication: Database triggers handle JSONB comparison
"""
from datetime import datetime
from typing import Dict, List, Any, Set, Optional
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

from api_client import HasakiAPIClient
from supabase_client import SupabaseStorage
from logger import setup_logger
from config import Config


class HasakiCrawler:
    """
    Professional Data Engineering Crawler
    
    Key Features:
    - Database-driven product discovery
    - Incremental snapshot (direct JSONB comparison in PostgreSQL)
    - Parallel processing with optimal worker pools
    - Comprehensive metrics: crawled vs inserted tracking
    - Multi-page review pagination per product (1-N pages)
    - Error handling, progress tracking, performance monitoring
    """
    
    # Performance tuning constants
    MAX_PRODUCT_WORKERS = 15  # Product API (lightweight, fast, 212 products)
    MAX_REVIEW_WORKERS = 6    # Review API (heavy, multi-page per product, reduce to avoid socket errors)
    PROGRESS_LOG_INTERVAL = 25  # Log every N products/reviews
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger()
        self.api_client = HasakiAPIClient()
        self.storage = SupabaseStorage()
        
        # Load target brand IDs
        self.brand_ids = set(self.config.load_brand_ids())
        
        # Thread-safe locks and collections
        self.lock = threading.Lock()
        self.crawled_products: Dict[int, int] = {}  # {product_id: snapshot_id}
        
        # Performance metrics
        self.metrics = {
            "started_at": time.time(),
            "started_at_iso": datetime.now().isoformat(),
            "db_query_time": 0.0,
            "product_crawl_time": 0.0,
            "review_crawl_time": 0.0,
        }
        
        # Statistics (thread-safe)
        self.stats = {
            "brand_ids": sorted(self.brand_ids),
            "target_brand_products": 0,
            "products_crawled": 0,
            "products_inserted": 0,
            "products_skipped": 0,
            "review_pages_crawled": 0,
            "review_pages_inserted": 0,
            "review_pages_skipped": 0,
            "errors": 0,
            "api_calls": 0
        }
    
    def _get_product_ids_from_db(self) -> Set[int]:
        """
        Query DISTINCT product IDs từ listing_api theo brand_ids
        
        Important: Use DISTINCT to avoid duplicate rows from multiple sessions
        Performance: Database query (~0.1-0.5s for thousands of products)
        """
        query_start = time.time()
        
        try:
            # Convert brand_ids to strings for comparison
            brand_ids_str = [str(bid) for bid in self.brand_ids]
            
            self.logger.info(f"  Target brands: {len(self.brand_ids)} brands")
            self.logger.info(f"  Querying listing_api table...")
            
            # CRITICAL FIX: Select distinct product_id to avoid duplicates
            # Each product may have multiple rows (from different sessions)
            result = self.storage.client.schema('raw').table('listing_api')\
                .select('product_id, brand_id')\
                .in_('brand_id', brand_ids_str)\
                .execute()
            
            query_time = time.time() - query_start
            self.metrics["db_query_time"] = query_time
            
            if result.data:
                # Extract unique product IDs (dedup in Python)
                product_ids = {int(row['product_id']) for row in result.data}
                
                # Debug info
                total_rows = len(result.data)
                unique_products = len(product_ids)
                
                self.logger.info(
                    f"  Found: {unique_products} unique products "
                    f"(from {total_rows} rows, query time: {query_time:.2f}s)"
                )
                
                if total_rows != unique_products:
                    self.logger.debug(
                        f"  Note: {total_rows - unique_products} duplicate rows "
                        f"(same product in multiple sessions)"
                    )
                
                return product_ids
            else:
                self.logger.warning("  No products found in database")
                self.logger.warning("  Please run: python crawl_listings.py")
                return set()
        
        except Exception as e:
            query_time = time.time() - query_start
            self.logger.error(
                f"Database query failed after {query_time:.2f}s: {e}"
            )
            return set()
    
    def crawl_all(self):
        """Main crawl workflow - Optimized (Read from DB)"""
        total_items = 0
        skipped_items = 0
        
        try:
            # Validate brands
            if not self.brand_ids:
                self.logger.error("ERROR: brands.txt rỗng!")
                self.logger.error("Chạy: python find_brands.py để tìm brand IDs")
                return
            
            self.logger.info(f"Target brands: {sorted(self.brand_ids)}")
            
            # Start session
            self.storage.start_session(api_type="full")
            
            # Step 1: Home API
            self.logger.info("=" * 60)
            self.logger.info("STEP 1: Home API")
            self.logger.info("=" * 60)
            
            try:
                home_data, home_metadata = self.api_client.get_home()
                if home_data:
                    home_id = self.storage.store_home(home_data)
                    if home_id:
                        self.logger.info(f"[OK] Home API stored (id: {home_id})")
                        total_items += 1
                    else:
                        self.logger.info("[SKIP] Home API skipped (no changes)")
                        skipped_items += 1
                else:
                    self.logger.error("Failed to fetch home API")
            except Exception as e:
                self.logger.error(f"Home API error: {e}")
                with self.lock:
                    self.stats["errors"] += 1
            
            # Step 2: Query product IDs từ database (NHANH!)
            self.logger.info("")
            self.logger.info("=" * 60)
            self.logger.info("STEP 2: Query Product IDs from Database")
            self.logger.info("=" * 60)
            
            target_product_ids = self._get_product_ids_from_db()
            
            if not target_product_ids:
                self.logger.error("Không tìm thấy products trong database!")
                self.logger.error("Chạy trước: python crawl_listings.py")
                self.storage.finish_session(status="failed", total_items=0, skipped_items=0)
                return
            
            self.logger.info(f"Found {len(target_product_ids)} target products in database")
            self.stats["target_brand_products"] = len(target_product_ids)
            
            # Step 3: Crawl Products (Parallel with optimized workers)
            if target_product_ids:
                self.logger.info("")
                self.logger.info("=" * 60)
                self.logger.info(f"STEP 3: Crawl {len(target_product_ids)} Products (Parallel)")
                self.logger.info("=" * 60)
                
                products_list = sorted(target_product_ids)
                
                self.logger.info(f"Workers: {self.MAX_PRODUCT_WORKERS} parallel")
                self.logger.info(f"Strategy: Incremental snapshot (only store on hash changes)")
                self.logger.info("")
                
                product_start = time.time()
                
                with ThreadPoolExecutor(max_workers=self.MAX_PRODUCT_WORKERS) as executor:
                    futures = {
                        executor.submit(self._crawl_product, pid): pid 
                        for pid in products_list
                    }
                    
                    for future in as_completed(futures):
                        try:
                            success = future.result()
                            if success:
                                with self.lock:
                                    self.stats["products_crawled"] += 1
                                
                                # Progress logging
                                if self.stats["products_crawled"] % self.PROGRESS_LOG_INTERVAL == 0:
                                    self.logger.info(
                                        f"  Progress: {self.stats['products_crawled']}/{len(products_list)} "
                                        f"products ({self.stats['products_crawled']*100//len(products_list)}%)"
                                    )
                        except Exception as e:
                            with self.lock:
                                self.stats["errors"] += 1
                            self.logger.debug(f"Product crawl error: {e}")
                
                product_time = time.time() - product_start
                self.metrics["product_crawl_time"] = product_time
                
                total_items += self.stats["products_crawled"]
                self.logger.info(
                    f"Step 3: {self.stats['products_crawled']} products crawled "
                    f"in {product_time:.1f}s "
                    f"({len(products_list)/product_time:.1f} products/s)"
                )
            else:
                self.logger.error("Không tìm thấy products trong database!")
                self.logger.error("Chạy trước: python crawl_listings.py")
            
            # Step 4: Crawl Reviews (Parallel with balanced workers)
            if self.crawled_products:
                self.logger.info("")
                self.logger.info("=" * 60)
                self.logger.info(f"STEP 4: Crawl Reviews for {len(self.crawled_products)} Products")
                self.logger.info("=" * 60)
                
                self.logger.info(f"Workers: {self.MAX_REVIEW_WORKERS} parallel")
                self.logger.info(f"Strategy: Paginated API (fetch all review pages per product)")
                self.logger.info("")
                
                review_start = time.time()
                products_with_reviews = 0
                
                with ThreadPoolExecutor(max_workers=self.MAX_REVIEW_WORKERS) as executor:
                    futures = {
                        executor.submit(self._crawl_reviews, pid, snapshot_id): pid
                        for pid, snapshot_id in self.crawled_products.items()
                    }
                    
                    for future in as_completed(futures):
                        try:
                            pages_crawled, pages_inserted = future.result()
                            with self.lock:
                                self.stats["review_pages_crawled"] += pages_crawled
                                self.stats["review_pages_inserted"] += pages_inserted
                                self.stats["review_pages_skipped"] += (pages_crawled - pages_inserted)
                                if pages_crawled > 0:
                                    products_with_reviews += 1
                            
                            # Progress logging
                            if products_with_reviews % self.PROGRESS_LOG_INTERVAL == 0 and products_with_reviews > 0:
                                self.logger.info(
                                    f"  Progress: {products_with_reviews}/{len(self.crawled_products)} "
                                    f"products, {self.stats['review_pages_crawled']} pages "
                                    f"(+{self.stats['review_pages_inserted']} new, "
                                    f"{self.stats['review_pages_skipped']} duplicates)"
                                )
                        except Exception as e:
                            with self.lock:
                                self.stats["errors"] += 1
                            self.logger.debug(f"Review crawl error: {e}")
                
                review_time = time.time() - review_start
                self.metrics["review_crawl_time"] = review_time
                
                self.logger.info(
                    f"Step 4: {self.stats['review_pages_crawled']} review pages crawled "
                    f"(+{self.stats['review_pages_inserted']} new, "
                    f"{self.stats['review_pages_skipped']} duplicates) "
                    f"from {products_with_reviews} products in {review_time:.1f}s"
                )
                total_items += self.stats["review_pages_inserted"]  # Chỉ đếm pages mới insert
            
            # Finish session and collect final metrics
            storage_stats = self.storage.get_stats()
            
            # Update product stats from storage (review stats already tracked above)
            self.stats["products_inserted"] = storage_stats['product_inserted']
            self.stats["products_skipped"] = storage_stats['product_skipped']
            
            # Note: review_pages_inserted và review_pages_skipped đã được track
            # trực tiếp trong ThreadPoolExecutor loop ở trên
            
            skipped_items = (
                storage_stats['home_skipped'] + 
                storage_stats['listing_skipped'] + 
                storage_stats['product_skipped'] +
                self.stats['review_pages_skipped']  # Dùng giá trị đã track
            )
            
            self.storage.finish_session("completed", total_items, skipped_items)
            self._print_summary()
        
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            self.storage.finish_session("failed", total_items, skipped_items)
            raise
    
    def _crawl_product(self, product_id: int) -> bool:
        """Crawl 1 product and track snapshot_id"""
        try:
            product_data, _ = self.api_client.get_product_detail(product_id)
            if product_data:
                # store_product returns snapshot_id if inserted, None if skipped
                snapshot_id = self.storage.store_product(product_id, product_data)
                if snapshot_id:
                    with self.lock:
                        self.crawled_products[product_id] = snapshot_id
                    return True
                else:
                    # Product skipped (no changes) - try to get existing snapshot_id
                    existing_snapshot = self.storage.get_latest_product_snapshot_id(product_id)
                    if existing_snapshot:
                        with self.lock:
                            self.crawled_products[product_id] = existing_snapshot
                        self.logger.debug(f"Product {product_id}: Using existing snapshot {existing_snapshot}")
                    else:
                        self.logger.warning(f"Product {product_id}: Skipped but no existing snapshot found!")
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error crawling product {product_id}: {e}")
            return False
    
    def _crawl_reviews(self, product_id: int, snapshot_id: int) -> tuple[int, int]:
        """
        Crawl reviews for 1 product
        Returns: (pages_crawled, pages_inserted)
        - pages_crawled: Số pages API trả về (có thể duplicate)
        - pages_inserted: Số pages mới insert vào DB
        """
        try:
            review_pages = self.api_client.get_product_reviews(product_id)
            if review_pages:
                return self.storage.store_reviews_for_product(
                    product_id, 
                    snapshot_id, 
                    review_pages
                )
            return 0, 0
        except Exception as e:
            self.logger.debug(f"Error crawling reviews for product {product_id}: {e}")
            return 0, 0
    
    def _print_summary(self):
        """
        Print comprehensive summary with performance metrics
        Professional DE standard: Include timing, throughput, efficiency metrics
        """
        total_time = time.time() - self.metrics["started_at"]
        total_minutes = int(total_time / 60)
        total_seconds = int(total_time % 60)
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("CRAWL COMPLETED - PERFORMANCE REPORT")
        self.logger.info("=" * 70)
        self.logger.info(f"Session ID: {self.storage.session_id}")
        self.logger.info(f"Duration: {total_minutes}m {total_seconds}s")
        self.logger.info(f"Target Brands: {len(self.brand_ids)} brands")
        self.logger.info("")
        
        # Data Summary
        self.logger.info("Data Summary:")
        self.logger.info(f"  Products discovered: {self.stats['target_brand_products']}")
        self.logger.info(f"  Products crawled: {self.stats['products_crawled']}")
        self.logger.info(f"  Review pages crawled: {self.stats['review_pages_crawled']} pages "
                       f"(from {len(self.crawled_products)} products)")
        self.logger.info("")
        
        # Storage Efficiency (Direct JSONB Comparison)
        total_api_items = self.stats['products_crawled'] + self.stats['review_pages_crawled']
        total_inserted = self.stats['products_inserted'] + self.stats['review_pages_inserted']
        total_skipped = self.stats['products_skipped'] + self.stats['review_pages_skipped']
        
        if total_api_items > 0:
            efficiency = (total_skipped / total_api_items) * 100
            self.logger.info("Storage Efficiency (Direct JSONB Comparison):")
            self.logger.info(f"  Products: +{self.stats['products_inserted']} new, "
                           f"{self.stats['products_skipped']} unchanged")
            self.logger.info(f"  Review pages: +{self.stats['review_pages_inserted']} new, "
                           f"{self.stats['review_pages_skipped']} duplicates")
            self.logger.info(f"  Total inserted: {total_inserted}")
            self.logger.info(f"  Total skipped: {total_skipped} ({efficiency:.1f}%)")
            
            # Review pages per product average
            if len(self.crawled_products) > 0:
                avg_pages = self.stats['review_pages_crawled'] / len(self.crawled_products)
                self.logger.info(f"  Avg review pages per product: {avg_pages:.1f}")
            self.logger.info("")
        
        # Performance Metrics
        self.logger.info("Performance Metrics:")
        self.logger.info(f"  DB Query: {self.metrics['db_query_time']:.2f}s")
        
        if self.metrics["product_crawl_time"] > 0:
            product_rate = self.stats['products_crawled'] / self.metrics["product_crawl_time"]
            self.logger.info(
                f"  Product Crawl: {self.metrics['product_crawl_time']:.1f}s "
                f"({product_rate:.1f} products/s)"
            )
        
        if self.metrics["review_crawl_time"] > 0:
            review_rate = self.stats['review_pages_crawled'] / self.metrics["review_crawl_time"]
            self.logger.info(
                f"  Review Crawl: {self.metrics['review_crawl_time']:.1f}s "
                f"({review_rate:.1f} pages/s)"
            )
        
        if self.stats['errors'] > 0:
            self.logger.info(f"  Errors: {self.stats['errors']}")
        
        self.logger.info("")
        
        # Next Steps
        self.logger.info("Maintenance:")
        self.logger.info("  • Run 'python crawl_listings.py' weekly to discover new products")
        self.logger.info("  • Run 'python crawler.py' daily for product/review updates")
        self.logger.info("=" * 70)


def main():
    """Entry point"""
    parser = argparse.ArgumentParser(description="Hasaki Crawler")
    parser.add_argument("--brands-file", default="brands.txt", help="Brands file path")
    args = parser.parse_args()
    
    try:
        if args.brands_file != "brands.txt":
            from pathlib import Path
            Config.BRANDS_FILE = Path(args.brands_file)
        
        crawler = HasakiCrawler()
        crawler.crawl_all()
        return 0
    
    except Exception as e:
        logger = setup_logger()
        logger.error(f"Crawler failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
