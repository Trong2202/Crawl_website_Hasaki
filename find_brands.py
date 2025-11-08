"""
Tool tìm brand IDs từ Hasaki API
Chạy file này để lấy danh sách brands và IDs
"""
import requests
from api_client import HasakiAPIClient
from config import Config


def find_brands():
    """Find all brand IDs from Hasaki API"""
    import sys
    import io
    
    # Fix encoding for Windows
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
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
    print(f"Sampling first 3 categories...")
    print()
    
    # Sample 3 categories để có nhiều brands
    sample_cats = all_leaves[:3]
    products = []
    
    for cat in sample_cats:
        cat_id = int(cat["id"])
        cat_name = cat["name"]
        print(f"  Fetching: {cat_name}...")
        
        listing_pages = client.get_product_ids_from_category(cat_id, cat_name)
        if listing_pages:
            page_data, _ = listing_pages[0]
            page_products = page_data.get("listing", [])
            products.extend(page_products)
            print(f"    Got {len(page_products)} products")
    
    print(f"\nTotal: {len(products)} products")
    
    print(f"Found {len(products)} products, extracting brands...\n")
    
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
    
    # Print results
    print("=" * 60)
    print(f"BRANDS FOUND (Sample from {len(products)} products):")
    print("=" * 60)
    
    for brand_id in sorted(brands_dict.keys()):
        brand_name = brands_dict[brand_id]
        print(f"{brand_id:6d}  # {brand_name}")
    
    print()
    print("=" * 60)
    print("HOW TO USE:")
    print("=" * 60)
    print("1. Copy brand IDs to brands.txt")
    print("2. Example:")
    print("   105    # CeraVe")
    print("   28     # Bioderma")
    print("   13     # L'Oreal")
    print()
    print("3. Run: python crawler.py")


if __name__ == "__main__":
    find_brands()

