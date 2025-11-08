"""
Configuration management
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()


class Config:
    """Application configuration"""
    
    # Project paths
    BASE_DIR = Path(__file__).parent
    BRANDS_FILE = BASE_DIR / "brands.txt"
    
    # Supabase (Required - from environment)
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "raw")
    
    # API Endpoints (Tên ngắn gọn cho Git Actions)
    HASAKI_HOME_API = os.getenv(
        "HOME_API",
        "https://hasaki.vn/wap/v2/master/?page=newHeaderHome"
    )
    HASAKI_LISTING_API = os.getenv(
        "LISTING_API",
        "https://hasaki.vn/wap/v2/catalog/category/get-listing-product?cat={}&p={}&product_list_limit=12&&more_data=1&lstType=1&lstId=0"
    )
    HASAKI_PRODUCT_API = os.getenv(
        "PRODUCT_API",
        "https://hasaki.vn/wap/v2/product/detail?id={}"
    )
    HASAKI_REVIEW_API = os.getenv(
        "REVIEW_API",
        "https://hasaki.vn/mobile/v3/detail/product/rating-reviews?product_id={product_id}&page={page}&size=5&sort=create&filter=filter_all&is_desktop=1"
    )
    
    # Crawl settings (Hardcoded - no limits for big data)
    # No rate limiting - crawl as fast as possible
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3
    BATCH_SIZE = 100
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        if not cls.SUPABASE_URL or not cls.SUPABASE_KEY:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in .env file"
            )
        if not cls.SUPABASE_SCHEMA:
            raise ValueError(
                "SUPABASE_SCHEMA must be set (recommended: 'raw')"
            )
    
    @classmethod
    def load_brand_ids(cls) -> List[int]:
        """
        Load brand IDs từ brands.txt
        Format: Mỗi dòng một ID, hoặc nhiều IDs cách nhau bởi dấu chấm phẩy
        Lines bắt đầu bằng # sẽ bị ignore
        """
        try:
            if not cls.BRANDS_FILE.exists():
                return []
            
            brand_ids = []
            with open(cls.BRANDS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # Skip comment lines và empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Hỗ trợ nhiều IDs trên 1 dòng cách nhau bởi ; hoặc ,
                    if ';' in line:
                        ids = line.split(';')
                    elif ',' in line:
                        ids = line.split(',')
                    else:
                        ids = [line]
                    
                    # Parse từng ID
                    for id_str in ids:
                        id_str = id_str.strip()
                        # Strip inline comments
                        if '#' in id_str:
                            id_str = id_str.split('#')[0].strip()
                        
                        if id_str:
                            try:
                                brand_ids.append(int(id_str))
                            except ValueError:
                                print(f"Warning: Invalid brand ID: {id_str}")
            
            return list(set(brand_ids))  # Remove duplicates
        
        except Exception as e:
            print(f"Warning: Failed to load brands.txt: {e}")
            return []
    
    @classmethod
    def init_directories(cls):
        """Create necessary directories - No longer needed"""
        pass

