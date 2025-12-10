-- ScyllaDB Initialization Script
-- Creates keyspace and tables with CDC enabled for change data capture

-- ============================================================================
-- Keyspace Configuration
-- ============================================================================
-- Using SimpleStrategy with RF=1 for development (use NetworkTopologyStrategy in production)
CREATE KEYSPACE IF NOT EXISTS app_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE app_data;

-- ============================================================================
-- Application Tables with CDC Enabled
-- ============================================================================

-- Users table: Core user data
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status TEXT,
    metadata MAP<TEXT, TEXT>
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400};

-- User email index for lookups
CREATE INDEX IF NOT EXISTS users_email_idx ON users (email);

-- Orders table: E-commerce order tracking
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    user_id UUID,
    order_number TEXT,
    order_date TIMESTAMP,
    total_amount DECIMAL,
    currency TEXT,
    status TEXT,
    shipping_address TEXT,
    billing_address TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400};

-- Order user_id index for queries
CREATE INDEX IF NOT EXISTS orders_user_id_idx ON orders (user_id);

-- Order items table: Line items for each order
CREATE TABLE IF NOT EXISTS order_items (
    order_id UUID,
    item_id UUID,
    product_id UUID,
    product_name TEXT,
    quantity INT,
    unit_price DECIMAL,
    total_price DECIMAL,
    created_at TIMESTAMP,
    PRIMARY KEY (order_id, item_id)
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400};

-- Products table: Product catalog
CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    sku TEXT,
    name TEXT,
    description TEXT,
    category TEXT,
    price DECIMAL,
    currency TEXT,
    stock_quantity INT,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    attributes MAP<TEXT, TEXT>
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400};

-- Product SKU index
CREATE INDEX IF NOT EXISTS products_sku_idx ON products (sku);

-- Product category index
CREATE INDEX IF NOT EXISTS products_category_idx ON products (category);

-- Inventory transactions table: Track stock movements
CREATE TABLE IF NOT EXISTS inventory_transactions (
    transaction_id TIMEUUID PRIMARY KEY,
    product_id UUID,
    transaction_type TEXT,
    quantity_change INT,
    previous_quantity INT,
    new_quantity INT,
    reference_id UUID,
    notes TEXT,
    created_at TIMESTAMP
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400};

-- ============================================================================
-- Monitoring and Metadata Tables
-- ============================================================================

-- CDC processing metadata: Track connector progress (optional, managed by connector)
CREATE TABLE IF NOT EXISTS cdc_metadata (
    table_name TEXT PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    last_processed_stream_id BLOB,
    records_processed BIGINT
);

-- ============================================================================
-- Sample Data for Testing and Development
-- ============================================================================
-- This section contains test data fixtures for validating the CDC pipeline
-- These records should replicate to PostgreSQL when the pipeline is running

-- Test Data Set 1: Sample Users (with various statuses and metadata)
INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440001, 'john_doe', 'john.doe@example.com', 'John', 'Doe', toTimestamp(now()), toTimestamp(now()), 'active', {'subscription': 'premium', 'last_login': '2025-12-09'});

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440002, 'jane_smith', 'jane.smith@example.com', 'Jane', 'Smith', toTimestamp(now()), toTimestamp(now()), 'active', {'subscription': 'basic', 'region': 'US-WEST'});

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440003, 'bob_wilson', 'bob.wilson@example.com', 'Bob', 'Wilson', toTimestamp(now()), toTimestamp(now()), 'active', {'subscription': 'premium', 'region': 'US-EAST'});

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440004, 'alice_jones', 'alice.jones@example.com', 'Alice', 'Jones', toTimestamp(now()), toTimestamp(now()), 'inactive', {'subscription': 'basic', 'reason': 'vacation'});

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440005, 'charlie_brown', 'charlie.brown@example.com', 'Charlie', 'Brown', toTimestamp(now()), toTimestamp(now()), 'active', {'subscription': 'trial', 'trial_end': '2025-12-31'});

-- Test Data Set 2: Sample Products (various categories and price points)
INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at, attributes)
VALUES (660e8400-e29b-41d4-a716-446655440001, 'LAPTOP-001', 'Professional Laptop', '15-inch laptop with 16GB RAM and 512GB SSD', 'Electronics', 1299.99, 'USD', 50, true, toTimestamp(now()), toTimestamp(now()), {'brand': 'TechPro', 'warranty': '2 years', 'color': 'silver'});

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at, attributes)
VALUES (660e8400-e29b-41d4-a716-446655440002, 'MOUSE-001', 'Wireless Mouse', 'Ergonomic wireless mouse with USB receiver', 'Electronics', 29.99, 'USD', 200, true, toTimestamp(now()), toTimestamp(now()), {'brand': 'MouseCo', 'warranty': '1 year', 'color': 'black'});

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at, attributes)
VALUES (660e8400-e29b-41d4-a716-446655440003, 'KEYBOARD-001', 'Mechanical Keyboard', 'RGB mechanical keyboard with Cherry MX switches', 'Electronics', 89.99, 'USD', 75, true, toTimestamp(now()), toTimestamp(now()), {'brand': 'KeyMaster', 'warranty': '1 year', 'switch_type': 'Cherry MX Red'});

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at, attributes)
VALUES (660e8400-e29b-41d4-a716-446655440004, 'MONITOR-001', '4K Monitor', '27-inch 4K UHD monitor with IPS panel', 'Electronics', 449.99, 'USD', 30, true, toTimestamp(now()), toTimestamp(now()), {'brand': 'ViewPro', 'warranty': '3 years', 'resolution': '3840x2160'});

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at, attributes)
VALUES (660e8400-e29b-41d4-a716-446655440005, 'HEADSET-001', 'Gaming Headset', 'Wireless gaming headset with 7.1 surround sound', 'Electronics', 129.99, 'USD', 100, true, toTimestamp(now()), toTimestamp(now()), {'brand': 'SoundWave', 'warranty': '2 years', 'connectivity': 'wireless'});

-- Test Data Set 3: Sample Orders (with different statuses)
INSERT INTO orders (order_id, user_id, order_number, order_date, total_amount, currency, status, shipping_address, billing_address, created_at, updated_at)
VALUES (770e8400-e29b-41d4-a716-446655440001, 550e8400-e29b-41d4-a716-446655440001, 'ORD-2025-001', toTimestamp(now()), 1329.98, 'USD', 'completed', '123 Main St, San Francisco, CA 94102', '123 Main St, San Francisco, CA 94102', toTimestamp(now()), toTimestamp(now()));

INSERT INTO orders (order_id, user_id, order_number, order_date, total_amount, currency, status, shipping_address, billing_address, created_at, updated_at)
VALUES (770e8400-e29b-41d4-a716-446655440002, 550e8400-e29b-41d4-a716-446655440002, 'ORD-2025-002', toTimestamp(now()), 119.98, 'USD', 'processing', '456 Oak Ave, New York, NY 10001', '456 Oak Ave, New York, NY 10001', toTimestamp(now()), toTimestamp(now()));

INSERT INTO orders (order_id, user_id, order_number, order_date, total_amount, currency, status, shipping_address, billing_address, created_at, updated_at)
VALUES (770e8400-e29b-41d4-a716-446655440003, 550e8400-e29b-41d4-a716-446655440003, 'ORD-2025-003', toTimestamp(now()), 579.97, 'USD', 'shipped', '789 Pine Ln, Boston, MA 02101', '789 Pine Ln, Boston, MA 02101', toTimestamp(now()), toTimestamp(now()));

-- Test Data Set 4: Sample Order Items (line items for orders)
INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440001, 880e8400-e29b-41d4-a716-446655440001, 660e8400-e29b-41d4-a716-446655440001, 'Professional Laptop', 1, 1299.99, 1299.99, toTimestamp(now()));

INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440001, 880e8400-e29b-41d4-a716-446655440002, 660e8400-e29b-41d4-a716-446655440002, 'Wireless Mouse', 1, 29.99, 29.99, toTimestamp(now()));

INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440002, 880e8400-e29b-41d4-a716-446655440003, 660e8400-e29b-41d4-a716-446655440003, 'Mechanical Keyboard', 1, 89.99, 89.99, toTimestamp(now()));

INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440002, 880e8400-e29b-41d4-a716-446655440004, 660e8400-e29b-41d4-a716-446655440002, 'Wireless Mouse', 1, 29.99, 29.99, toTimestamp(now()));

INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440003, 880e8400-e29b-41d4-a716-446655440005, 660e8400-e29b-41d4-a716-446655440004, '4K Monitor', 1, 449.99, 449.99, toTimestamp(now()));

INSERT INTO order_items (order_id, item_id, product_id, product_name, quantity, unit_price, total_price, created_at)
VALUES (770e8400-e29b-41d4-a716-446655440003, 880e8400-e29b-41d4-a716-446655440006, 660e8400-e29b-41d4-a716-446655440005, 'Gaming Headset', 1, 129.99, 129.99, toTimestamp(now()));

-- Test Data Set 5: Sample Inventory Transactions
INSERT INTO inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, previous_quantity, new_quantity, reference_id, notes, created_at)
VALUES (now(), 660e8400-e29b-41d4-a716-446655440001, 'sale', -1, 51, 50, 770e8400-e29b-41d4-a716-446655440001, 'Order ORD-2025-001', toTimestamp(now()));

INSERT INTO inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, previous_quantity, new_quantity, reference_id, notes, created_at)
VALUES (now(), 660e8400-e29b-41d4-a716-446655440002, 'sale', -2, 202, 200, 770e8400-e29b-41d4-a716-446655440001, 'Multiple orders', toTimestamp(now()));

INSERT INTO inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, previous_quantity, new_quantity, reference_id, notes, created_at)
VALUES (now(), 660e8400-e29b-41d4-a716-446655440003, 'sale', -1, 76, 75, 770e8400-e29b-41d4-a716-446655440002, 'Order ORD-2025-002', toTimestamp(now()));

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Verify CDC is enabled (check system tables)
-- SELECT table_name, cdc FROM system_schema.tables WHERE keyspace_name = 'app_data';

-- Check record counts
-- SELECT COUNT(*) FROM users;
-- SELECT COUNT(*) FROM products;
