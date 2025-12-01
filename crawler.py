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

    MAX_PRODUCT_WORKERS = 10  # Product API (lightest: 1 request per product, small response)
    MAX_REVIEW_WORKERS = 20   # Review API (heaviest: multi-page per product, large responses, pagination)
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
                return product_ids
            else:
                self.logger.warning("  > No products found in database")
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
                self.logger.error("ERROR: brands.txt rỗng! Chạy: python find_brands.py")
                return
            
            self.logger.info(f"\nHasaki Crawler - Brands: {sorted(self.brand_ids)}")
            
            # Start session
            self.storage.start_session(api_type="full")
            
            # Step 1: Home API
            self.logger.info("\n[1/4] Crawl Home API...")
            
            try:
                home_data, home_metadata = self.api_client.get_home()
                if home_data:
                    home_id = self.storage.store_home(home_data)
                    if home_id:
                        self.logger.info("  > Stored home data")
                        total_items += 1
                    else:
                        self.logger.info("  > No changes (skipped)")
                        skipped_items += 1
                else:
                    self.logger.error("  > Failed to fetch")
            except Exception as e:
                self.logger.error(f"  > Error: {e}")
                with self.lock:
                    self.stats["errors"] += 1
            
            # Step 2: Query product IDs từ database
            self.logger.info("\n[2/4] Load Products from Database...")
            
            target_product_ids = self._get_product_ids_from_db()
            
            if not target_product_ids:
                self.logger.error("  > Không tìm thấy products! Chạy trước: python crawl_listings.py")
                self.storage.finish_session(status="failed", total_items=0, skipped_items=0)
                return
            
            self.logger.info(f"  > Loaded {len(target_product_ids)} products")
            self.stats["target_brand_products"] = len(target_product_ids)
            
            # Step 3: Crawl Products
            if target_product_ids:
                self.logger.info(f"\n[3/4] Crawl Products ({len(target_product_ids)} items, {self.MAX_PRODUCT_WORKERS} workers)...")
                
                products_list = sorted(target_product_ids)
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
                                    progress_pct = self.stats['products_crawled']*100//len(products_list)
                                    self.logger.info(f"  > {self.stats['products_crawled']}/{len(products_list)} ({progress_pct}%)")
                        except Exception as e:
                            with self.lock:
                                self.stats["errors"] += 1
                            self.logger.debug(f"Product crawl error: {e}")
                
                product_time = time.time() - product_start
                self.metrics["product_crawl_time"] = product_time
                
                total_items += self.stats["products_crawled"]
                rate = len(products_list)/product_time if product_time > 0 else 0
                self.logger.info(f"  > Done in {product_time:.1f}s ({rate:.1f} items/s)")
            else:
                self.logger.error("  > Không tìm thấy products! Chạy trước: python crawl_listings.py")
            
            # Step 4: Crawl Reviews
            if self.crawled_products:
                self.logger.info(f"\n[4/4] Crawl Reviews ({len(self.crawled_products)} products, {self.MAX_REVIEW_WORKERS} workers)...")
                
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
                                    f"  > {products_with_reviews}/{len(self.crawled_products)} products, "
                                    f"{self.stats['review_pages_crawled']} pages (+{self.stats['review_pages_inserted']} new)"
                                )
                        except Exception as e:
                            with self.lock:
                                self.stats["errors"] += 1
                            self.logger.debug(f"Review crawl error: {e}")
                
                review_time = time.time() - review_start
                self.metrics["review_crawl_time"] = review_time
                
                self.logger.info(
                    f"  > Done: {self.stats['review_pages_crawled']} pages "
                    f"(+{self.stats['review_pages_inserted']} new) in {review_time:.1f}s"
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
        """Print concise summary"""
        total_time = time.time() - self.metrics["started_at"]
        total_minutes = int(total_time / 60)
        total_seconds = int(total_time % 60)
        
        total_inserted = self.stats['products_inserted'] + self.stats['review_pages_inserted']
        total_skipped = self.stats['products_skipped'] + self.stats['review_pages_skipped']
        
        self.logger.info("\n" + "=" * 50)
        self.logger.info(f"COMPLETED - Session {self.storage.session_id}")
        self.logger.info("=" * 50)
        self.logger.info(f"Time: {total_minutes}m {total_seconds}s | Brands: {len(self.brand_ids)}")
        self.logger.info(f"Products: {self.stats['products_crawled']} crawled, +{self.stats['products_inserted']} new")
        self.logger.info(f"Reviews: {self.stats['review_pages_crawled']} pages, +{self.stats['review_pages_inserted']} new")
        self.logger.info(f"Total: +{total_inserted} new, {total_skipped} unchanged")
        if self.stats['errors'] > 0:
            self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info("=" * 50)


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
