-- Expected PostgreSQL State After CDC Replication
-- This file contains the expected data in PostgreSQL after replication from ScyllaDB

-- Expected users count
-- SELECT COUNT(*) FROM cdc_data.users; -- Expected: 5

-- Expected users data (excluding CDC metadata fields)
-- User 1
-- user_id: 11111111-1111-1111-1111-111111111111
-- username: test_user_1
-- email: user1@test.com
-- first_name: Test
-- last_name: User1
-- status: active

-- User 2
-- user_id: 22222222-2222-2222-2222-222222222222
-- username: test_user_2
-- email: user2@test.com
-- first_name: Test
-- last_name: User2
-- status: active

-- User 3
-- user_id: 33333333-3333-3333-3333-333333333333
-- username: test_user_3
-- email: user3@test.com
-- first_name: Test
-- last_name: User3
-- status: inactive

-- User 4
-- user_id: 44444444-4444-4444-4444-444444444444
-- username: test_user_4
-- email: user4@test.com
-- first_name: Test
-- last_name: User4
-- status: active

-- User 5
-- user_id: 55555555-5555-5555-5555-555555555555
-- username: test_user_5
-- email: user5@test.com
-- first_name: Test
-- last_name: User5
-- status: pending

-- Expected products count
-- SELECT COUNT(*) FROM cdc_data.products; -- Expected: 5

-- Expected products data
-- Product A
-- product_id: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
-- product_name: Test Product A
-- price: 19.99
-- stock_quantity: 100
-- category: Electronics
-- is_active: true

-- Product B
-- product_id: bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
-- product_name: Test Product B
-- price: 29.99
-- stock_quantity: 50
-- category: Books
-- is_active: true

-- Product C
-- product_id: cccccccc-cccc-cccc-cccc-cccccccccccc
-- product_name: Test Product C
-- price: 39.99
-- stock_quantity: 75
-- category: Clothing
-- is_active: true

-- Product D
-- product_id: dddddddd-dddd-dddd-dddd-dddddddddddd
-- product_name: Test Product D
-- price: 49.99
-- stock_quantity: 25
-- category: Electronics
-- is_active: false

-- Product E
-- product_id: eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee
-- product_name: Test Product E
-- price: 59.99
-- stock_quantity: 10
-- category: Home
-- is_active: true

-- Expected orders count
-- SELECT COUNT(*) FROM cdc_data.orders; -- Expected: 5

-- Expected orders data
-- Order 1
-- order_id: f0000000-0000-0000-0000-000000000001
-- user_id: 11111111-1111-1111-1111-111111111111
-- total_amount: 99.95
-- status: pending

-- Order 2
-- order_id: f0000000-0000-0000-0000-000000000002
-- user_id: 22222222-2222-2222-2222-222222222222
-- total_amount: 149.90
-- status: processing

-- Order 3
-- order_id: f0000000-0000-0000-0000-000000000003
-- user_id: 11111111-1111-1111-1111-111111111111
-- total_amount: 79.98
-- status: shipped

-- Order 4
-- order_id: f0000000-0000-0000-0000-000000000004
-- user_id: 33333333-3333-3333-3333-333333333333
-- total_amount: 199.99
-- status: delivered

-- Order 5
-- order_id: f0000000-0000-0000-0000-000000000005
-- user_id: 44444444-4444-4444-4444-444444444444
-- total_amount: 59.99
-- status: cancelled

-- Expected order_items count
-- SELECT COUNT(*) FROM cdc_data.order_items; -- Expected: 9

-- Expected inventory_transactions count
-- SELECT COUNT(*) FROM cdc_data.inventory_transactions; -- Expected: 5

-- Validation Queries
-- These queries can be used to verify the expected state

-- Verify all users replicated
SELECT
    user_id::text,
    username,
    email,
    status
FROM cdc_data.users
WHERE user_id IN (
    '11111111-1111-1111-1111-111111111111',
    '22222222-2222-2222-2222-222222222222',
    '33333333-3333-3333-3333-333333333333',
    '44444444-4444-4444-4444-444444444444',
    '55555555-5555-5555-5555-555555555555'
)
ORDER BY username;
-- Expected rows: 5

-- Verify all products replicated
SELECT
    product_id::text,
    product_name,
    price,
    category,
    is_active
FROM cdc_data.products
WHERE product_id IN (
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    'cccccccc-cccc-cccc-cccc-cccccccccccc',
    'dddddddd-dddd-dddd-dddd-dddddddddddd',
    'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'
)
ORDER BY product_name;
-- Expected rows: 5

-- Verify all orders replicated
SELECT
    order_id::text,
    user_id::text,
    total_amount,
    status
FROM cdc_data.orders
WHERE order_id IN (
    'f0000000-0000-0000-0000-000000000001',
    'f0000000-0000-0000-0000-000000000002',
    'f0000000-0000-0000-0000-000000000003',
    'f0000000-0000-0000-0000-000000000004',
    'f0000000-0000-0000-0000-000000000005'
)
ORDER BY order_id;
-- Expected rows: 5

-- Verify order items replicated
SELECT
    item_id::text,
    order_id::text,
    product_id::text,
    quantity,
    unit_price,
    subtotal
FROM cdc_data.order_items
WHERE order_id IN (
    'f0000000-0000-0000-0000-000000000001',
    'f0000000-0000-0000-0000-000000000002',
    'f0000000-0000-0000-0000-000000000003',
    'f0000000-0000-0000-0000-000000000004',
    'f0000000-0000-0000-0000-000000000005'
)
ORDER BY item_id;
-- Expected rows: 9

-- Verify inventory transactions replicated
SELECT
    transaction_id::text,
    product_id::text,
    transaction_type,
    quantity_change
FROM cdc_data.inventory_transactions
WHERE transaction_id IN (
    'd0000000-0000-0000-0000-000000000001',
    'd0000000-0000-0000-0000-000000000002',
    'd0000000-0000-0000-0000-000000000003',
    'd0000000-0000-0000-0000-000000000004',
    'd0000000-0000-0000-0000-000000000005'
)
ORDER BY transaction_id;
-- Expected rows: 5

-- Verify referential integrity
SELECT
    o.order_id::text,
    o.user_id::text,
    u.username,
    COUNT(oi.item_id) as item_count
FROM cdc_data.orders o
LEFT JOIN cdc_data.users u ON o.user_id = u.user_id
LEFT JOIN cdc_data.order_items oi ON o.order_id = oi.order_id
WHERE o.order_id IN (
    'f0000000-0000-0000-0000-000000000001',
    'f0000000-0000-0000-0000-000000000002',
    'f0000000-0000-0000-0000-000000000003',
    'f0000000-0000-0000-0000-000000000004',
    'f0000000-0000-0000-0000-000000000005'
)
GROUP BY o.order_id, o.user_id, u.username
ORDER BY o.order_id;
-- Expected: All orders should have corresponding users and item counts

-- Verify CDC metadata fields exist
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'cdc_data'
  AND table_name = 'users'
  AND column_name IN ('cdc_timestamp', 'cdc_source')
ORDER BY column_name;
-- Expected rows: 2 (cdc_timestamp, cdc_source)
