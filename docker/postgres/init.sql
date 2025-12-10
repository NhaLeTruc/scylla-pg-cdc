-- PostgreSQL Data Warehouse Initialization Script
-- Creates schema, tables, and indexes for replicated data from ScyllaDB

-- ============================================================================
-- Schema Configuration
-- ============================================================================

-- Create dedicated schema for CDC data
CREATE SCHEMA IF NOT EXISTS cdc_data;

-- Set search path
SET search_path TO cdc_data, public;

-- ============================================================================
-- Data Warehouse Tables
-- ============================================================================

-- Users dimension table
CREATE TABLE IF NOT EXISTS cdc_data.users (
    user_id UUID PRIMARY KEY,
    username VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status VARCHAR(50),
    metadata JSONB,
    -- Metadata columns for CDC tracking
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_source VARCHAR(100),
    cdc_stream_id BYTEA
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_users_email ON cdc_data.users(email);
CREATE INDEX IF NOT EXISTS idx_users_status ON cdc_data.users(status);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON cdc_data.users(created_at);
CREATE INDEX IF NOT EXISTS idx_users_cdc_timestamp ON cdc_data.users(cdc_timestamp);

-- Orders fact table
CREATE TABLE IF NOT EXISTS cdc_data.orders (
    order_id UUID PRIMARY KEY,
    user_id UUID,
    order_number VARCHAR(100),
    order_date TIMESTAMP,
    total_amount NUMERIC(12, 2),
    currency VARCHAR(3),
    status VARCHAR(50),
    shipping_address TEXT,
    billing_address TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    -- CDC metadata
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_source VARCHAR(100),
    cdc_stream_id BYTEA,
    -- Foreign key constraint
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES cdc_data.users(user_id) ON DELETE SET NULL
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON cdc_data.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON cdc_data.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON cdc_data.orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_cdc_timestamp ON cdc_data.orders(cdc_timestamp);

-- Order items fact table
CREATE TABLE IF NOT EXISTS cdc_data.order_items (
    order_id UUID,
    item_id UUID,
    product_id UUID,
    product_name VARCHAR(500),
    quantity INTEGER,
    unit_price NUMERIC(12, 2),
    total_price NUMERIC(12, 2),
    created_at TIMESTAMP,
    -- CDC metadata
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_source VARCHAR(100),
    cdc_stream_id BYTEA,
    PRIMARY KEY (order_id, item_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON cdc_data.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON cdc_data.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_cdc_timestamp ON cdc_data.order_items(cdc_timestamp);

-- Products dimension table
CREATE TABLE IF NOT EXISTS cdc_data.products (
    product_id UUID PRIMARY KEY,
    sku VARCHAR(100) UNIQUE,
    name VARCHAR(500),
    description TEXT,
    category VARCHAR(255),
    price NUMERIC(12, 2),
    currency VARCHAR(3),
    stock_quantity INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    attributes JSONB,
    -- CDC metadata
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_source VARCHAR(100),
    cdc_stream_id BYTEA
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_products_sku ON cdc_data.products(sku);
CREATE INDEX IF NOT EXISTS idx_products_category ON cdc_data.products(category);
CREATE INDEX IF NOT EXISTS idx_products_is_active ON cdc_data.products(is_active);
CREATE INDEX IF NOT EXISTS idx_products_cdc_timestamp ON cdc_data.products(cdc_timestamp);

-- Inventory transactions fact table
CREATE TABLE IF NOT EXISTS cdc_data.inventory_transactions (
    transaction_id UUID PRIMARY KEY,
    product_id UUID,
    transaction_type VARCHAR(50),
    quantity_change INTEGER,
    previous_quantity INTEGER,
    new_quantity INTEGER,
    reference_id UUID,
    notes TEXT,
    created_at TIMESTAMP,
    -- CDC metadata
    cdc_operation VARCHAR(10),
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_source VARCHAR(100),
    cdc_stream_id BYTEA
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON cdc_data.inventory_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_created_at ON cdc_data.inventory_transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_inventory_transaction_type ON cdc_data.inventory_transactions(transaction_type);

-- ============================================================================
-- Audit and Monitoring Tables
-- ============================================================================

-- CDC replication audit log
CREATE TABLE IF NOT EXISTS cdc_data.replication_audit (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(10) NOT NULL,
    record_count INTEGER DEFAULT 1,
    source_timestamp TIMESTAMP,
    target_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    kafka_topic VARCHAR(255),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    processing_duration_ms INTEGER,
    error_message TEXT,
    status VARCHAR(50) DEFAULT 'success'
);

CREATE INDEX IF NOT EXISTS idx_audit_table_name ON cdc_data.replication_audit(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_target_timestamp ON cdc_data.replication_audit(target_timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_status ON cdc_data.replication_audit(status);

-- Data quality monitoring
CREATE TABLE IF NOT EXISTS cdc_data.data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value NUMERIC,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_quality_table_name ON cdc_data.data_quality_metrics(table_name);
CREATE INDEX IF NOT EXISTS idx_quality_recorded_at ON cdc_data.data_quality_metrics(recorded_at);

-- ============================================================================
-- Materialized Views for Analytics
-- ============================================================================

-- Daily order summary
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_data.daily_order_summary AS
SELECT
    DATE(order_date) AS order_day,
    status,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    MIN(total_amount) AS min_order_value,
    MAX(total_amount) AS max_order_value
FROM cdc_data.orders
GROUP BY DATE(order_date), status
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_summary_unique ON cdc_data.daily_order_summary(order_day, status);

-- Product inventory status
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_data.product_inventory_status AS
SELECT
    p.product_id,
    p.sku,
    p.name,
    p.category,
    p.stock_quantity,
    p.is_active,
    COUNT(oi.item_id) AS total_orders,
    SUM(oi.quantity) AS total_quantity_sold
FROM cdc_data.products p
LEFT JOIN cdc_data.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.sku, p.name, p.category, p.stock_quantity, p.is_active
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_product_inventory_unique ON cdc_data.product_inventory_status(product_id);

-- ============================================================================
-- Functions and Triggers
-- ============================================================================

-- Function to update materialized views
CREATE OR REPLACE FUNCTION cdc_data.refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_data.daily_order_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY cdc_data.product_inventory_status;
END;
$$ LANGUAGE plpgsql;

-- Function to record data quality metrics
CREATE OR REPLACE FUNCTION cdc_data.record_row_count(p_table_name VARCHAR)
RETURNS void AS $$
DECLARE
    v_count INTEGER;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM cdc_data.%I', p_table_name) INTO v_count;
    INSERT INTO cdc_data.data_quality_metrics (table_name, metric_name, metric_value)
    VALUES (p_table_name, 'row_count', v_count);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Permissions and Security
-- ============================================================================

-- Create read-only role for analytics
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_reader') THEN
        CREATE ROLE cdc_reader;
    END IF;
END
$$;
GRANT USAGE ON SCHEMA cdc_data TO cdc_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA cdc_data TO cdc_reader;
-- Grant select on materialized views individually (ALL MATERIALIZED VIEWS not supported in PG15)
GRANT SELECT ON cdc_data.daily_order_summary TO cdc_reader;
GRANT SELECT ON cdc_data.product_inventory_status TO cdc_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA cdc_data GRANT SELECT ON TABLES TO cdc_reader;

-- Create write role for CDC connector
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_writer') THEN
        CREATE ROLE cdc_writer;
    END IF;
END
$$;
GRANT USAGE ON SCHEMA cdc_data TO cdc_writer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cdc_data TO cdc_writer;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA cdc_data TO cdc_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA cdc_data GRANT INSERT, UPDATE, DELETE ON TABLES TO cdc_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA cdc_data GRANT USAGE ON SEQUENCES TO cdc_writer;

-- ============================================================================
-- Verification and Statistics
-- ============================================================================

-- Analyze tables for query optimization
ANALYZE cdc_data.users;
ANALYZE cdc_data.orders;
ANALYZE cdc_data.order_items;
ANALYZE cdc_data.products;
ANALYZE cdc_data.inventory_transactions;

-- Display table information
DO $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'CDC Data Warehouse Schema Initialized Successfully';
    RAISE NOTICE '================================================';
    FOR r IN
        SELECT table_name,
               pg_size_pretty(pg_total_relation_size('cdc_data.' || table_name)) AS size
        FROM information_schema.tables
        WHERE table_schema = 'cdc_data' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    LOOP
        RAISE NOTICE 'Table: % (Size: %)', r.table_name, r.size;
    END LOOP;
END $$;

-- ============================================================================
-- Expected Results for Validation (After CDC Replication)
-- ============================================================================
-- These queries can be used to verify that CDC replication is working correctly
-- After the pipeline is running, the replicated data from ScyllaDB should match these expectations

-- Expected User Counts by Status
-- After replication: Should have 4 active users and 1 inactive user
CREATE OR REPLACE VIEW cdc_data.expected_user_count_by_status AS
SELECT
    'active' AS status,
    4 AS expected_count,
    'Should have 4 active users (john_doe, jane_smith, bob_wilson, charlie_brown)' AS description
UNION ALL
SELECT
    'inactive' AS status,
    1 AS expected_count,
    'Should have 1 inactive user (alice_jones)' AS description;

-- Validation Query: Check actual vs expected user counts by status
CREATE OR REPLACE VIEW cdc_data.validate_user_counts AS
SELECT
    e.status AS metric,
    e.expected_count::TEXT AS expected_value,
    COALESCE(a.actual_count, 0)::TEXT AS actual_value,
    CASE
        WHEN COALESCE(a.actual_count, 0) = e.expected_count THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    e.description
FROM cdc_data.expected_user_count_by_status e
LEFT JOIN (
    SELECT status, COUNT(*) AS actual_count
    FROM cdc_data.users
    GROUP BY status
) a ON e.status = a.status;

-- Expected Product Counts
-- After replication: Should have 5 products total
CREATE OR REPLACE VIEW cdc_data.expected_product_metrics AS
SELECT
    'total_products' AS metric,
    5 AS expected_value,
    'Should have 5 products replicated from ScyllaDB' AS description
UNION ALL
SELECT
    'active_products' AS metric,
    5 AS expected_value,
    'All 5 products should be active' AS description
UNION ALL
SELECT
    'total_stock' AS metric,
    455 AS expected_value,
    'Total stock quantity across all products (50+200+75+30+100)' AS description;

-- Validation Query: Check actual vs expected product metrics
CREATE OR REPLACE VIEW cdc_data.validate_product_metrics AS
SELECT
    e.metric,
    e.expected_value::TEXT AS expected_value,
    CASE e.metric
        WHEN 'total_products' THEN (SELECT COUNT(*)::TEXT FROM cdc_data.products)
        WHEN 'active_products' THEN (SELECT COUNT(*)::TEXT FROM cdc_data.products WHERE is_active = true)
        WHEN 'total_stock' THEN (SELECT COALESCE(SUM(stock_quantity), 0)::TEXT FROM cdc_data.products)
        ELSE '0'
    END AS actual_value,
    CASE
        WHEN CASE e.metric
            WHEN 'total_products' THEN (SELECT COUNT(*)::INTEGER FROM cdc_data.products)
            WHEN 'active_products' THEN (SELECT COUNT(*)::INTEGER FROM cdc_data.products WHERE is_active = true)
            WHEN 'total_stock' THEN (SELECT COALESCE(SUM(stock_quantity), 0)::INTEGER FROM cdc_data.products)
            ELSE 0
        END = e.expected_value THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    e.description
FROM cdc_data.expected_product_metrics e;

-- Expected Order Metrics
-- After replication: Should have 3 orders with specific totals
CREATE OR REPLACE VIEW cdc_data.expected_order_metrics AS
SELECT
    'total_orders' AS metric,
    3 AS expected_value,
    'Should have 3 orders replicated' AS description
UNION ALL
SELECT
    'total_revenue' AS metric,
    2029.93 AS expected_value,
    'Total revenue from all orders (1329.98 + 119.98 + 579.97)' AS description
UNION ALL
SELECT
    'avg_order_value' AS metric,
    676.64 AS expected_value,
    'Average order value (rounded to 2 decimals)' AS description;

-- Validation Query: Check actual vs expected order metrics
CREATE OR REPLACE VIEW cdc_data.validate_order_metrics AS
SELECT
    e.metric,
    e.expected_value::TEXT AS expected_value,
    CASE e.metric
        WHEN 'total_orders' THEN (SELECT COALESCE(COUNT(*), 0)::TEXT FROM cdc_data.orders)
        WHEN 'total_revenue' THEN (SELECT COALESCE(ROUND(SUM(total_amount)::NUMERIC, 2), 0)::TEXT FROM cdc_data.orders)
        WHEN 'avg_order_value' THEN (SELECT COALESCE(ROUND(AVG(total_amount)::NUMERIC, 2), 0)::TEXT FROM cdc_data.orders)
        ELSE '0'
    END AS actual_value,
    CASE
        WHEN ABS(
            CASE e.metric
                WHEN 'total_orders' THEN (SELECT COALESCE(COUNT(*), 0)::NUMERIC FROM cdc_data.orders)
                WHEN 'total_revenue' THEN (SELECT COALESCE(ROUND(SUM(total_amount)::NUMERIC, 2), 0) FROM cdc_data.orders)
                WHEN 'avg_order_value' THEN (SELECT COALESCE(ROUND(AVG(total_amount)::NUMERIC, 2), 0) FROM cdc_data.orders)
                ELSE 0
            END - e.expected_value
        ) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    e.description
FROM cdc_data.expected_order_metrics e;

-- Expected Order Items Count
-- After replication: Should have 6 order items
CREATE OR REPLACE VIEW cdc_data.expected_order_item_metrics AS
SELECT
    'total_order_items' AS metric,
    6 AS expected_value,
    'Should have 6 order items replicated' AS description;

-- Validation Query: Check actual vs expected order item counts
CREATE OR REPLACE VIEW cdc_data.validate_order_item_metrics AS
SELECT
    e.metric,
    e.expected_value::TEXT AS expected_value,
    (SELECT COUNT(*)::TEXT FROM cdc_data.order_items) AS actual_value,
    CASE
        WHEN (SELECT COUNT(*)::INTEGER FROM cdc_data.order_items) = e.expected_value THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    e.description
FROM cdc_data.expected_order_item_metrics e;

-- Expected Inventory Transactions
-- After replication: Should have 3 inventory transactions
CREATE OR REPLACE VIEW cdc_data.expected_inventory_metrics AS
SELECT
    'total_transactions' AS metric,
    3 AS expected_value,
    'Should have 3 inventory transactions replicated' AS description;

-- Validation Query: Check actual vs expected inventory transaction counts
CREATE OR REPLACE VIEW cdc_data.validate_inventory_metrics AS
SELECT
    e.metric,
    e.expected_value::TEXT AS expected_value,
    (SELECT COUNT(*)::TEXT FROM cdc_data.inventory_transactions) AS actual_value,
    CASE
        WHEN (SELECT COUNT(*)::INTEGER FROM cdc_data.inventory_transactions) = e.expected_value THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    e.description
FROM cdc_data.expected_inventory_metrics e;

-- Master Validation View: All Validations in One Place
CREATE OR REPLACE VIEW cdc_data.validation_summary AS
SELECT 'Users' AS category, * FROM cdc_data.validate_user_counts
UNION ALL
SELECT 'Products' AS category, metric, expected_value::TEXT, actual_value::TEXT, validation_status, description FROM cdc_data.validate_product_metrics
UNION ALL
SELECT 'Orders' AS category, metric, expected_value::TEXT, actual_value::TEXT, validation_status, description FROM cdc_data.validate_order_metrics
UNION ALL
SELECT 'Order Items' AS category, metric, expected_value::TEXT, actual_value::TEXT, validation_status, description FROM cdc_data.validate_order_item_metrics
UNION ALL
SELECT 'Inventory' AS category, metric, expected_value::TEXT, actual_value::TEXT, validation_status, description FROM cdc_data.validate_inventory_metrics;

-- ============================================================================
-- Validation Helper Functions
-- ============================================================================

-- Function to run all validations and return summary
CREATE OR REPLACE FUNCTION cdc_data.run_validation_checks()
RETURNS TABLE (
    category VARCHAR,
    check_name VARCHAR,
    expected VARCHAR,
    actual VARCHAR,
    status VARCHAR,
    details TEXT
) AS $$
BEGIN
    RETURN QUERY SELECT * FROM cdc_data.validation_summary;
END;
$$ LANGUAGE plpgsql;

-- Function to check if CDC replication is complete (all validations pass)
CREATE OR REPLACE FUNCTION cdc_data.is_replication_complete()
RETURNS BOOLEAN AS $$
DECLARE
    failed_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO failed_count
    FROM cdc_data.validation_summary
    WHERE validation_status = 'FAIL';

    RETURN failed_count = 0;
END;
$$ LANGUAGE plpgsql;

-- Function to get replication completeness percentage
CREATE OR REPLACE FUNCTION cdc_data.get_replication_completeness()
RETURNS NUMERIC AS $$
DECLARE
    total_checks INTEGER;
    passed_checks INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_checks FROM cdc_data.validation_summary;
    SELECT COUNT(*) INTO passed_checks FROM cdc_data.validation_summary WHERE validation_status = 'PASS';

    IF total_checks = 0 THEN
        RETURN 0;
    END IF;

    RETURN ROUND((passed_checks::NUMERIC / total_checks::NUMERIC) * 100, 2);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Quick Validation Commands (for manual testing)
-- ============================================================================

-- To check all validations at once:
-- SELECT * FROM cdc_data.validation_summary ORDER BY category, status, description;

-- To check if replication is complete:
-- SELECT cdc_data.is_replication_complete() AS all_checks_passed;

-- To check replication completeness percentage:
-- SELECT cdc_data.get_replication_completeness() || '%' AS completeness;

-- To run validation checks as a function:
-- SELECT * FROM cdc_data.run_validation_checks() ORDER BY category, status;
