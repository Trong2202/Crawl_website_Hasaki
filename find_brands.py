"""
Tool tìm brand IDs từ Hasaki API
Chạy file này để lấy danh sách brands và IDs
"""
import requests
from api_client import HasakiAPIClient
from config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def find_brands():
    """Find all brand IDs from Hasaki API"""
    import sys
    
    # Fix encoding for Windows
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8')
    
    print("=" * 60)
    print("FIND BRAND IDs FROM HASAKI")
    print("=" * 60)
    print()
    
    config = Config()
    client = HasakiAPIClient()
    
    brands_dict = {}
    
    # Fetch home
    print("Fetching categories...")
    home_data, _ = client.get_categories()
    
    if not home_data:
        print("Error: Failed to fetch home API")
        return
    
    categories = home_data.get("cate_menu", [])
    
    # Get all leaf categories
    def get_all_leaves(cat_list):
        leaves = []
        for cat in cat_list:
            if cat.get("name") in ["Mỹ Phẩm High-End"]:
                continue
            if "child" in cat and cat["child"]:
                leaves.extend(get_all_leaves(cat["child"]))
            else:
                leaves.append(cat)
        return leaves
    
    all_leaves = get_all_leaves(categories)
    
    if not all_leaves:
        print("Error: No categories found")
        return
    
    print(f"Found {len(all_leaves)} categories")
    print(f"⚡ Fetching ALL categories in parallel (25 workers)...")
    print()
    
    # Lấy TẤT CẢ categories song song để có full brands
    products = []
    start_time = time.time()
    
    # Helper function để fetch 1 category
    def fetch_category(idx_cat):
        idx, cat = idx_cat
        cat_id = int(cat["id"])
        cat_name = cat["name"]
        
        try:
            listing_pages = client.get_product_ids_from_category(cat_id, cat_name)
            if listing_pages:
                page_data, _ = listing_pages[0]
                page_products = page_data.get("listing", [])
                return (idx, cat_name, page_products)
        except Exception as e:
            print(f"    ⚠️  Error fetching {cat_name}: {e}")
            return (idx, cat_name, [])
        
        return (idx, cat_name, [])
    
    # Fetch parallel với ThreadPoolExecutor
    total_cats = len(all_leaves)
    completed = 0
    
    with ThreadPoolExecutor(max_workers=25) as executor:
        # Submit all tasks
        future_to_cat = {
            executor.submit(fetch_category, (idx, cat)): (idx, cat)
            for idx, cat in enumerate(all_leaves, 1)
        }
        
        # Process as completed
        for future in as_completed(future_to_cat):
            idx, cat_name, page_products = future.result()
            products.extend(page_products)
            completed += 1
            
            # Progress indicator
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (total_cats - completed) / rate if rate > 0 else 0
            
            print(f"  [{completed}/{total_cats}] ✓ {cat_name} ({len(page_products)} products) | "
                  f"⏱️ {rate:.1f} cat/s | ETA: {eta:.0f}s")
    
    elapsed_total = time.time() - start_time
    print(f"\n⚡ Hoàn tất {total_cats} categories trong {elapsed_total:.1f}s "
          f"({total_cats/elapsed_total:.1f} cat/s)")
    print(f"📦 Tổng: {len(products)} products")

    
    print(f"Extracting brands...\n")
    
    for product in products:
        brand = product.get("brand")
        if isinstance(brand, dict):
            brand_id = brand.get("id")
            brand_name = brand.get("name")
            if brand_id and brand_name:
                try:
                    brand_id = int(brand_id)
                    if brand_id not in brands_dict:
                        brands_dict[brand_id] = brand_name
                except:
                    pass
    
    # Write to file
    output_file = "all_brands.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for brand_id in sorted(brands_dict.keys()):
            brand_name = brands_dict[brand_id]
            f.write(f"{brand_id:6d}  # {brand_name}\n")
    
    # Print summary
    print()
    print("=" * 60)
    print(f"✅ HOÀN TẤT! Tìm thấy {len(brands_dict)} brands duy nhất")
    print("=" * 60)
    print()
    print(f"📄 Đã lưu {len(brands_dict)} brands vào: {output_file}")
    print()
    print("🔍 Preview (10 brands đầu tiên):")
    for brand_id in sorted(brands_dict.keys())[:10]:
        brand_name = brands_dict[brand_id]
        print(f"  {brand_id:6d}  # {brand_name}")
    if len(brands_dict) > 10:
        print(f"  ... và {len(brands_dict) - 10} brands khác")
    print()
    print("=" * 60)
    print("📌 HƯỚNG DẪN SỬ DỤNG:")
    print("=" * 60)
    print(f"1. Mở file: {output_file}")
    print(f"2. Copy các brand IDs bạn muốn crawl vào: brands.txt")
    print("3. Ví dụ brands.txt:")
    print("   105    # CeraVe")
    print("   28     # Bioderma")
    print("   13     # L'Oreal")
    print()
    print("4. Chạy crawler: python crawler.py")
    print("=" * 60)


if __name__ == "__main__":
    find_brands()

