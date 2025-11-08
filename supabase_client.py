"""
Supabase client - Match với schema.sql mới
Incremental snapshot: chỉ lưu khi có thay đổi
"""
import uuid
import hashlib
import json
import time
from typing import Dict, Any, Optional
from supabase import create_client, Client

from logger import setup_logger
from config import Config


class SupabaseStorage:
    """
    Supabase storage với incremental snapshot strategy
    - Chỉ lưu khi data thay đổi
    - Functions tự động check hash và skip duplicates
    """
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger()
        self.client: Client = self._init_client()
        self.session_id: Optional[uuid.UUID] = None
        
        # Statistics
        self.stats = {
            'home_inserted': 0,
            'home_skipped': 0,
            'listing_inserted': 0,
            'listing_skipped': 0,
            'product_inserted': 0,
            'product_skipped': 0,
            'review_inserted': 0,
            'review_skipped': 0,
            'errors': 0
        }
    
    def _init_client(self) -> Client:
        """Initialize Supabase client"""
        try:
            self.config.validate()
            from supabase.lib.client_options import ClientOptions
            
            options = ClientOptions(
                schema=self.config.SUPABASE_SCHEMA,
                auto_refresh_token=False,
                persist_session=False
            )
            
            client = create_client(
                self.config.SUPABASE_URL,
                self.config.SUPABASE_KEY,
                options=options
            )
            self.logger.info(f"Supabase client initialized (schema: {self.config.SUPABASE_SCHEMA})")
            return client
        except Exception as e:
            self.logger.error(f"Failed to initialize Supabase: {e}")
            raise
    
    # REMOVED: _calculate_hash()
    # Database triggers handle all hash calculation and deduplication
    # No need for Python to calculate hash (avoids mismatch)
    
    def start_session(self, api_type: str = "full") -> uuid.UUID:
        """Bắt đầu crawl session"""
        try:
            result = self.client.rpc('create_crawl_session', {
                'p_source_name': 'hasaki'
            }).execute()
            
            if result.data:
                self.session_id = uuid.UUID(result.data)
                self.logger.info(f"Session started: {self.session_id}")
                return self.session_id
            else:
                raise ValueError("No session_id returned")
        
        except Exception as e:
            self.logger.error(f"Failed to start session: {e}")
            raise
    
    def finish_session(
        self,
        status: str = "completed",
        total_items: int = 0,
        skipped_items: int = 0
    ) -> bool:
        """Kết thúc session"""
        if not self.session_id:
            self.logger.warning("No active session")
            return False
        
        try:
            self.client.rpc('complete_crawl_session', {
                'p_session_id': str(self.session_id),
                'p_status': status
            }).execute()
            
            self.logger.info(
                f"Session finished: {self.session_id} "
                f"(status: {status})"
            )
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to finish session: {e}")
            return False
    
    def store_home(self, data: Dict[str, Any]) -> bool:
        """
        Lưu Home API data
        Dùng safe_insert function (ON CONFLICT DO NOTHING)
        """
        if not self.session_id:
            self.logger.warning("No active session")
            return False
        
        try:
            # Dùng safe_insert function - tự động handle conflict
            result = self.client.rpc('safe_insert_home_api', {
                'p_session_id': str(self.session_id),
                'p_source_name': 'hasaki',
                'p_data': data
            }).execute()
            
            # Nếu return NULL = duplicate, nếu có ID = inserted
            if result.data is not None:
                self.stats['home_inserted'] += 1
                self.logger.info(f"Home data inserted (id: {result.data})")
                return True
            else:
                self.stats['home_skipped'] += 1
                self.logger.debug("Home data skipped (duplicate)")
                return True
        
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"Failed to store home: {e}")
            return False
    
    
    def store_product(
        self,
        product_id: int,
        data: Dict[str, Any]
    ) -> Optional[int]:
        """
        Lưu Product API data
        Schema mới: CHỈ lưu data + data_hash (không có bought/price)
        Trigger tự động check data_hash để phát hiện thay đổi
        Returns: product_snapshot_id nếu có thay đổi, None nếu không đổi
        """
        if not self.session_id:
            self.logger.warning("No active session")
            return None
        
        # Retry logic for Supabase RPC
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Dùng safe_insert_product_api RPC function
                result = self.client.rpc('safe_insert_product_api', {
                    'p_session_id': str(self.session_id),
                    'p_source_name': 'hasaki',
                    'p_product_id': str(product_id),
                    'p_data': data
                }).execute()
                
                # Function returns id nếu inserted, NULL nếu trigger reject
                if result.data is not None:
                    snapshot_id = int(result.data) if result.data else None
                    if snapshot_id:
                        self.stats['product_inserted'] += 1
                        self.logger.debug(f"Product inserted (id: {product_id}, snapshot: {snapshot_id})")
                        return snapshot_id
                else:
                    # Trigger rejected (no changes)
                    self.stats['product_skipped'] += 1
                    self.logger.debug(f"Product skipped (id: {product_id}, no changes)")
                    return None
            
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    self.stats['errors'] += 1
                    self.logger.error(f"Failed to store product {product_id}: {e}")
                    return None
        
        return None
    
    # REMOVED: _check_review_exists()
    # Database trigger handles all duplicate detection
    # No need for Python-side checking
    
    def store_review_page(
        self,
        product_id: int,
        product_snapshot_id: int,
        page_number: int,
        page_data: Dict[str, Any]
    ) -> Optional[str]:
        """
        Lưu FULL PAGE JSON của reviews (toàn bộ response API)
        
        Args:
            product_id: Product ID
            product_snapshot_id: Product snapshot ID
            page_number: Page number (1, 2, 3...) để track và dedup
            page_data: Full API response JSON (có cả rating, sort_params, reviews, total)
        
        Returns: 'inserted' | 'duplicate' | None (error)
        
        Note: Schema mới lưu page_number (field: pages) để track từng trang
        """
        if not self.session_id:
            self.logger.warning("No active session")
            return None
        
        # Retry logic for Supabase RPC (handle socket errors)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Schema mới: safe_insert_review_api(p_data, p_product_id, p_product_snapshot_id, p_session_id, p_total)
                # p_total là page number (field 'pages' trong table)
                result = self.client.rpc('safe_insert_review_api', {
                    'p_data': page_data,
                    'p_product_id': str(product_id),
                    'p_product_snapshot_id': product_snapshot_id,
                    'p_session_id': str(self.session_id),
                    'p_total': page_number  # Page number (1, 2, 3...)
                }).execute()
                
                # Nếu return NULL = duplicate (trigger reject)
                if result.data is not None:
                    self.stats['review_inserted'] += 1
                    return 'inserted'
                else:
                    self.stats['review_skipped'] += 1
                    return 'duplicate'
            
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # 100ms, 200ms delay
                    continue
                else:
                    # Final attempt failed
                    self.stats['errors'] += 1
                    self.logger.error(f"Failed to store review page {page_number} for product {product_id}: {e}")
                    return None
        
        return None
    
    def store_reviews_for_product(
        self,
        product_id: int,
        product_snapshot_id: int,
        review_pages: list
    ) -> tuple[int, int]:
        """
        Lưu TOÀN BỘ review pages của product (FULL PAGE JSON)
        
        Strategy - Store FULL PAGE JSON:
        1. Process all pages sequentially  
        2. Store FULL API response for each page (không tách reviews)
        3. Database trigger handles deduplication by JSONB comparison
        
        Mỗi page lưu TOÀN BỘ JSON response bao gồm:
        - status
        - data.rating
        - data.sort_params
        - data.reviews (array)
        - data.total
        
        Returns: (total_pages_crawled, total_pages_inserted)
        - total_pages_crawled: Số pages API trả về
        - total_pages_inserted: Số pages mới insert (excluding duplicates)
        """
        if not review_pages:
            return 0, 0
        
        total_crawled = len(review_pages)
        total_inserted = 0
        
        for page_data, metadata, page_number in review_pages:
            # Lưu FULL PAGE JSON với page number (1, 2, 3...)
            # Trigger tự dedup bằng JSONB comparison
            result = self.store_review_page(
                product_id, 
                product_snapshot_id,
                page_number,  # Page number để track
                page_data     # Full JSON response
            )
            
            if result == 'inserted':
                total_inserted += 1
            
        return total_crawled, total_inserted
    
    def get_latest_product_snapshot_id(self, product_id: int) -> Optional[int]:
        """
        Lấy product_snapshot_id mới nhất của product
        """
        # Retry logic for Supabase RPC
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.rpc('get_latest_product_snapshot_id', {
                    'p_product_id': str(product_id)
                }).execute()
                
                return result.data if result.data else None
            
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    self.logger.error(f"Failed to get snapshot id for product {product_id}: {e}")
                    return None
        
        return None
    
    def get_stats(self) -> Dict[str, int]:
        """Lấy statistics"""
        return self.stats.copy()
