"""
API client for Hasaki with retry logic
"""
import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger import setup_logger
from config import Config


class HasakiAPIClient:
    """Client for Hasaki API with proper retry and rate limiting"""
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger()
        self.session = self._create_session()
        self.request_count = 0
    
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        })
        
        return session
    
    def _make_request(
        self, 
        url: str,
        return_metadata: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with error handling và track metadata
        Returns: response JSON hoặc dict với 'data' và 'metadata' nếu return_metadata=True
        """
        # Retry logic for socket errors (WinError 10035)
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                response = self.session.get(
                    url,
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response_time_ms = int((time.time() - start_time) * 1000)
                
                response.raise_for_status()
                self.request_count += 1
                
                data = response.json()
                
                if return_metadata:
                    return {
                        'data': data,
                        'metadata': {
                            'http_status': response.status_code,
                            'response_time_ms': response_time_ms,
                            'request_url': url
                        }
                    }
                
                return data
            
            except (requests.exceptions.RequestException, OSError, ConnectionError) as e:
                last_error = e
                # Retry on socket/connection errors
                if attempt < max_retries - 1:
                    time.sleep(0.05 * (attempt + 1))  # Small delay: 50ms, 100ms, 150ms
                    continue
                else:
                    # Final attempt failed
                    self.logger.debug(f"Request failed after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def get_home(self) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Fetch home page data
        Returns: (home_data, metadata_dict)
        """
        result = self._make_request(self.config.HASAKI_HOME_API, return_metadata=True)
        if not result:
            return None, None
        
        return result['data'], result['metadata']
    
    def get_categories(self) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Fetch all categories from home API
        Returns: (full_response_data, metadata_dict)
        """
        self.logger.info("Fetching categories...")
        
        result = self._make_request(self.config.HASAKI_HOME_API, return_metadata=True)
        if not result:
            return None, None
        
        return result['data'], result['metadata']
    
    def get_product_ids_from_category(
        self,
        category_id: int,
        category_name: str
    ) -> List[tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        Get all products from a category
        Returns: List of tuples (listing_data, metadata)
        """
        self.logger.info(f"Fetching product listings from category: {category_name}")
        
        all_listings = []
        page = 1
        
        while True:
            url = self.config.HASAKI_LISTING_API.format(category_id, page)
            result = self._make_request(url, return_metadata=True)
            
            if not result:
                break
            
            data = result['data']
            metadata = result['metadata']
            
            listings = data.get("listing", [])
            if not listings:
                break
            
            # Store full listing response với metadata
            all_listings.append((data, metadata))
            page += 1
        
        self.logger.info(
            f"Found {len(all_listings)} pages in {category_name}"
        )
        return all_listings
    
    def get_product_detail(self, product_id: int) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Fetch full product detail
        Returns: (product_data, metadata_dict)
        """
        url = self.config.HASAKI_PRODUCT_API.format(product_id)
        result = self._make_request(url, return_metadata=True)
        
        if not result:
            return None, None
        
        return result['data'], result['metadata']
    
    def _fetch_review_page(
        self, 
        product_id: int, 
        page: int
    ) -> Optional[tuple[Dict[str, Any], Dict[str, Any], int]]:
        """
        Fetch single review page (helper for parallel crawling)
        Returns: (data, metadata, page_num) or None if empty
        """
        url = self.config.HASAKI_REVIEW_API.format(product_id=product_id, page=page)
        result = self._make_request(url, return_metadata=True)
        
        if not result:
            return None
        
        data = result['data']
        metadata = result['metadata']
        
        # API structure: response.data.reviews
        reviews_data = data.get("data", {})
        if isinstance(reviews_data, dict):
            reviews = reviews_data.get("reviews", [])
        else:
            reviews = []
        
        if not reviews or len(reviews) == 0:
            return None
        
        return (data, metadata, page)
    
    def get_product_reviews(
        self,
        product_id: int,
        max_pages: int = 50
    ) -> List[tuple[Dict[str, Any], Dict[str, Any], int]]:
        """
        Get all reviews for a product (SEQUENTIAL with smart stopping)
        
        Strategy:
        1. Crawl pages sequentially (1, 2, 3...)
        2. Calculate total pages from first response (total / page_size)
        3. Stop when: empty reviews OR reached calculated pages OR duplicate content
        
        IMPORTANT: Hasaki API BUG - Khi hết reviews, API không trả về rỗng 
        mà lặp lại nội dung trang cuối! Phải dùng total để tính số pages.
        
        Args:
            product_id: Product ID
            max_pages: Maximum pages to crawl (safety limit)
        
        Returns: List of tuples (review_page_data, metadata, page_number)
        """
        all_reviews = []
        page = 1
        consecutive_failures = 0
        max_consecutive_failures = 3
        calculated_max_pages = None
        PAGE_SIZE = 5  # Hasaki API trả 5 reviews/page
        
        while page <= max_pages:
            url = self.config.HASAKI_REVIEW_API.format(product_id=product_id, page=page)
            result = self._make_request(url, return_metadata=True)
            
            if not result:
                consecutive_failures += 1
                self.logger.debug(
                    f"Product {product_id} page {page}: Request failed "
                    f"({consecutive_failures}/{max_consecutive_failures})"
                )
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.warning(
                        f"Product {product_id}: Stopped at page {page} "
                        f"(consecutive failures: {consecutive_failures})"
                    )
                    break
                page += 1
                continue
            
            # Reset counter khi thành công
            consecutive_failures = 0
            
            data = result['data']
            metadata = result['metadata']
            
            # Extract reviews and total
            reviews_data = data.get("data", {})
            if isinstance(reviews_data, dict):
                reviews = reviews_data.get("reviews", [])
                total_reviews = reviews_data.get("total", 0)
            else:
                reviews = []
                total_reviews = 0
            
            # Calculate max pages from first response
            if page == 1 and total_reviews > 0:
                calculated_max_pages = (total_reviews + PAGE_SIZE - 1) // PAGE_SIZE  # Ceiling division
                self.logger.debug(
                    f"Product {product_id}: {total_reviews} reviews → {calculated_max_pages} pages expected"
                )
            
            # Stop conditions
            if not reviews or len(reviews) == 0:
                self.logger.debug(f"Product {product_id}: Empty reviews at page {page}")
                break
            
            # Check if we've reached calculated max (prevent API bug: repeating last page)
            if calculated_max_pages and page > calculated_max_pages:
                self.logger.debug(
                    f"Product {product_id}: Reached calculated max pages ({calculated_max_pages})"
                )
                break
            
            # Lưu page data
            all_reviews.append((data, metadata, page))
            page += 1
        
        if all_reviews:
            self.logger.debug(
                f"Product {product_id}: {len(all_reviews)} review pages collected "
                f"(expected: {calculated_max_pages or 'unknown'})"
            )
        
        return all_reviews
    
    def get_product_reviews_sequential(
        self,
        product_id: int
    ) -> List[tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        Get all reviews for a product (LEGACY - Sequential pagination)
        Use for fallback or debugging only
        """
        all_reviews = []
        page = 1
        
        while True:
            url = self.config.HASAKI_REVIEW_API.format(product_id=product_id, page=page)
            result = self._make_request(url, return_metadata=True)
            
            if not result:
                break
            
            data = result['data']
            metadata = result['metadata']
            
            # API structure: response.data.reviews
            reviews_data = data.get("data", {})
            if isinstance(reviews_data, dict):
                reviews = reviews_data.get("reviews", [])
            else:
                reviews = []
            
            if not reviews or len(reviews) == 0:
                break
            
            all_reviews.append((data, metadata))
            page += 1
        
        if all_reviews:
            self.logger.info(
                f"Found {len(all_reviews)} pages of reviews for product {product_id}"
            )
        
        return all_reviews
    
    def get_stats(self) -> Dict[str, int]:
        """Get client statistics"""
        return {
            "total_requests": self.request_count
        }

