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
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 86400}
AND CLUSTERING ORDER BY (transaction_id DESC);

-- ============================================================================
-- Monitoring and Metadata Tables
-- ============================================================================

-- CDC processing metadata: Track connector progress (optional, managed by connector)
CREATE TABLE IF NOT EXISTS cdc_metadata (
    table_name TEXT PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    last_processed_stream_id BLOB,
    records_processed COUNTER
);

-- ============================================================================
-- Sample Data for Testing
-- ============================================================================

-- Insert sample users
INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (uuid(), 'john_doe', 'john.doe@example.com', 'John', 'Doe', toTimestamp(now()), toTimestamp(now()), 'active');

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (uuid(), 'jane_smith', 'jane.smith@example.com', 'Jane', 'Smith', toTimestamp(now()), toTimestamp(now()), 'active');

INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (uuid(), 'bob_wilson', 'bob.wilson@example.com', 'Bob', 'Wilson', toTimestamp(now()), toTimestamp(now()), 'active');

-- Insert sample products
INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at)
VALUES (uuid(), 'LAPTOP-001', 'Professional Laptop', '15-inch laptop with 16GB RAM', 'Electronics', 1299.99, 'USD', 50, true, toTimestamp(now()), toTimestamp(now()));

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at)
VALUES (uuid(), 'MOUSE-001', 'Wireless Mouse', 'Ergonomic wireless mouse', 'Electronics', 29.99, 'USD', 200, true, toTimestamp(now()), toTimestamp(now()));

INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at)
VALUES (uuid(), 'KEYBOARD-001', 'Mechanical Keyboard', 'RGB mechanical keyboard', 'Electronics', 89.99, 'USD', 75, true, toTimestamp(now()), toTimestamp(now()));

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Verify CDC is enabled (check system tables)
-- SELECT table_name, cdc FROM system_schema.tables WHERE keyspace_name = 'app_data';

-- Check record counts
-- SELECT COUNT(*) FROM users;
-- SELECT COUNT(*) FROM products;
