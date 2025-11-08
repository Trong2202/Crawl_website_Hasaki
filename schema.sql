-- =====================================================
-- SUPABASE DATABASE SCHEMA FOR WEB CRAWLING DATA LAKE
-- Schema: raw (Raw Data Storage with Incremental Snapshot)
-- Insert only when data changes (direct JSONB comparison)
-- Timezone: Asia/Ho_Chi_Minh (UTC+7)
-- =====================================================

-- Set default timezone cho database
ALTER DATABASE postgres SET timezone = 'Asia/Ho_Chi_Minh';

CREATE SCHEMA IF NOT EXISTS raw;

-- =====================================================
-- 1. TẠO CÁC BẢNG CHÍNH
-- =====================================================

CREATE TABLE raw.crawl_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    time INTERVAL NULL,
    total_products INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'running' 
        CHECK (status IN ('running', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.home_api (
    home_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.listing_api (
    listing_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    brand_id VARCHAR(100) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(product_id)
);

CREATE TABLE raw.product_api (
    id BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    source_name VARCHAR(100) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.review_api (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    product_snapshot_id BIGINT NOT NULL REFERENCES raw.product_api(id) ON DELETE CASCADE,
    session_id UUID NOT NULL REFERENCES raw.crawl_sessions(session_id) ON DELETE CASCADE,
    pages INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- 2. TẠO CÁC INDEX
-- =====================================================

CREATE INDEX idx_sessions_source_status ON raw.crawl_sessions(source_name, status);
CREATE INDEX idx_sessions_created_at ON raw.crawl_sessions(created_at DESC);

CREATE INDEX idx_home_session ON raw.home_api(session_id);
CREATE INDEX idx_home_source ON raw.home_api(source_name);
CREATE INDEX idx_home_data_gin ON raw.home_api USING gin(data);

CREATE INDEX idx_listing_session ON raw.listing_api(session_id);
CREATE INDEX idx_listing_product ON raw.listing_api(product_id, source_name);
CREATE INDEX idx_listing_brand ON raw.listing_api(brand_id) WHERE brand_id IS NOT NULL;

CREATE INDEX idx_product_id ON raw.product_api(product_id);
CREATE INDEX idx_product_session ON raw.product_api(session_id);
CREATE INDEX idx_product_source_id ON raw.product_api(source_name, product_id);
CREATE INDEX idx_product_created_at ON raw.product_api(created_at DESC);
CREATE INDEX idx_product_data_gin ON raw.product_api USING gin(data);
CREATE INDEX idx_product_id_created ON raw.product_api(product_id, created_at DESC);

CREATE INDEX idx_review_product ON raw.review_api(product_id);
CREATE INDEX idx_review_snapshot ON raw.review_api(product_snapshot_id);
CREATE INDEX idx_review_created_at ON raw.review_api(created_at DESC);
CREATE INDEX idx_review_data_gin ON raw.review_api USING gin(data);
CREATE INDEX idx_review_pages ON raw.review_api(product_id, pages);

-- =====================================================
-- 3. TRIGGER: TỰ ĐỘNG CẬP NHẬT THỜI GIAN CRAWL
-- =====================================================

CREATE OR REPLACE FUNCTION raw.update_crawl_session_time()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.finished_at IS NOT NULL AND NEW.started_at IS NOT NULL THEN
        NEW.time = NEW.finished_at - NEW.started_at;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_session_time
BEFORE UPDATE ON raw.crawl_sessions
FOR EACH ROW
WHEN (NEW.finished_at IS NOT NULL AND OLD.finished_at IS NULL)
EXECUTE FUNCTION raw.update_crawl_session_time();

-- =====================================================
-- 4. TRIGGER: CHỐNG TRÙNG LẶP HOME_API
-- =====================================================

CREATE OR REPLACE FUNCTION raw.check_home_api_duplicate()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM raw.home_api 
        WHERE data = NEW.data
    ) THEN
        RETURN NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_home_api_dedup
BEFORE INSERT ON raw.home_api
FOR EACH ROW
EXECUTE FUNCTION raw.check_home_api_duplicate();

-- =====================================================
-- 5. TRIGGER: CHỈ INSERT KHI CÓ THAY ĐỔI DATA
-- =====================================================

CREATE OR REPLACE FUNCTION raw.check_product_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Kiểm tra xem đã có data giống hệt trong database chưa
    IF EXISTS (
        SELECT 1 FROM raw.product_api 
        WHERE product_id = NEW.product_id
        AND data = NEW.data
    ) THEN
        -- Data giống nhau, bỏ qua insert
        RETURN NULL;
    END IF;
    
    -- Data khác hoặc chưa có, cho phép insert
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_product_change_detection
BEFORE INSERT ON raw.product_api
FOR EACH ROW
EXECUTE FUNCTION raw.check_product_changes();

-- =====================================================
-- 6. TRIGGER: CHỐNG TRÙNG LẶP REVIEW_API
-- =====================================================

CREATE OR REPLACE FUNCTION raw.check_review_duplicate()
RETURNS TRIGGER AS $$
BEGIN
    -- Check duplicate dựa trên (product_id, pages) và data
    -- Phải match cả 3: cùng product, cùng page, cùng nội dung
    IF EXISTS (
        SELECT 1 FROM raw.review_api 
        WHERE product_id = NEW.product_id
        AND pages = NEW.pages
        AND data = NEW.data
    ) THEN
        RETURN NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_review_dedup
BEFORE INSERT ON raw.review_api
FOR EACH ROW
EXECUTE FUNCTION raw.check_review_duplicate();

-- =====================================================
-- 7. TRIGGER: TỰ ĐỘNG CẬP NHẬT TOTAL_PRODUCTS
-- =====================================================

CREATE OR REPLACE FUNCTION raw.update_session_total_products()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE raw.crawl_sessions
    SET total_products = total_products + 1
    WHERE session_id = NEW.session_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_count_listings
AFTER INSERT ON raw.listing_api
FOR EACH ROW
EXECUTE FUNCTION raw.update_session_total_products();

CREATE TRIGGER trigger_count_products
AFTER INSERT ON raw.product_api
FOR EACH ROW
EXECUTE FUNCTION raw.update_session_total_products();

-- =====================================================
-- 8. HELPER FUNCTIONS
-- =====================================================

CREATE OR REPLACE FUNCTION raw.create_crawl_session(p_source_name VARCHAR)
RETURNS UUID AS $$
DECLARE
    v_session_id UUID;
BEGIN
    INSERT INTO raw.crawl_sessions (source_name)
    VALUES (p_source_name)
    RETURNING session_id INTO v_session_id;
    
    RETURN v_session_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.complete_crawl_session(p_session_id UUID, p_status VARCHAR DEFAULT 'completed')
RETURNS VOID AS $$
BEGIN
    UPDATE raw.crawl_sessions
    SET finished_at = NOW(),
        status = p_status
    WHERE session_id = p_session_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.get_latest_product_snapshot_id(p_product_id VARCHAR)
RETURNS BIGINT AS $$
DECLARE
    v_snapshot_id BIGINT;
BEGIN
    SELECT id INTO v_snapshot_id
    FROM raw.product_api
    WHERE product_id = p_product_id
    ORDER BY created_at DESC
    LIMIT 1;
    
    RETURN v_snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 9. HELPER FUNCTION: AN TOÀN INSERT VỚI DEDUP
-- =====================================================

CREATE OR REPLACE FUNCTION raw.safe_insert_home_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_data JSONB
)
RETURNS BIGINT AS $$
DECLARE
    v_home_id BIGINT;
BEGIN
    INSERT INTO raw.home_api (session_id, source_name, data)
    VALUES (p_session_id, p_source_name, p_data)
    RETURNING home_id INTO v_home_id;
    
    RETURN v_home_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_listing_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_product_id VARCHAR,
    p_brand_id VARCHAR DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_listing_id BIGINT;
BEGIN
    INSERT INTO raw.listing_api (session_id, source_name, product_id, brand_id)
    VALUES (p_session_id, p_source_name, p_product_id, p_brand_id)
    ON CONFLICT (product_id) DO NOTHING
    RETURNING listing_id INTO v_listing_id;
    
    RETURN v_listing_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_review_api(
    p_data JSONB,
    p_product_id VARCHAR,
    p_product_snapshot_id BIGINT,
    p_session_id UUID,
    p_total INTEGER DEFAULT 0
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    INSERT INTO raw.review_api (data, product_id, product_snapshot_id, session_id, pages)
    VALUES (p_data, p_product_id, p_product_snapshot_id, p_session_id, p_total)
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.safe_insert_product_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_product_id VARCHAR,
    p_data JSONB
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    INSERT INTO raw.product_api (product_id, session_id, source_name, data)
    VALUES (p_product_id, p_session_id, p_source_name, p_data)
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION raw.batch_insert_listing_api(
    p_session_id UUID,
    p_source_name VARCHAR,
    p_products JSONB
)
RETURNS INTEGER AS $$
DECLARE
    v_product JSONB;
    v_inserted INTEGER := 0;
    v_result BIGINT;
BEGIN
    FOR v_product IN SELECT * FROM jsonb_array_elements(p_products)
    LOOP
        INSERT INTO raw.listing_api (session_id, source_name, product_id, brand_id)
        VALUES (
            p_session_id,
            p_source_name,
            v_product->>'product_id',
            v_product->>'brand_id'
        )
        ON CONFLICT (product_id) DO NOTHING
        RETURNING listing_id INTO v_result;
        
        IF v_result IS NOT NULL THEN
            v_inserted := v_inserted + 1;
        END IF;
    END LOOP;
    
    RETURN v_inserted;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- THÔNG BÁO KẾT QUẢ
-- =====================================================

SELECT 'Schema raw with direct JSONB comparison created successfully!' as status;
SELECT tablename FROM pg_tables WHERE schemaname = 'raw' ORDER BY tablename;