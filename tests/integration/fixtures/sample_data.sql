-- Sample Test Data for ScyllaDB (CQL)
-- This file contains test data to be loaded into ScyllaDB for integration tests

-- Users table test data
INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (11111111-1111-1111-1111-111111111111, 'test_user_1', 'user1@test.com', 'Test', 'User1', toTimestamp(now()), toTimestamp(now()), 'active');

INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (22222222-2222-2222-2222-222222222222, 'test_user_2', 'user2@test.com', 'Test', 'User2', toTimestamp(now()), toTimestamp(now()), 'active');

INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (33333333-3333-3333-3333-333333333333, 'test_user_3', 'user3@test.com', 'Test', 'User3', toTimestamp(now()), toTimestamp(now()), 'inactive');

INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (44444444-4444-4444-4444-444444444444, 'test_user_4', 'user4@test.com', 'Test', 'User4', toTimestamp(now()), toTimestamp(now()), 'active');

INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
VALUES (55555555-5555-5555-5555-555555555555, 'test_user_5', 'user5@test.com', 'Test', 'User5', toTimestamp(now()), toTimestamp(now()), 'pending');

-- Products table test data
INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'Test Product A', 'Description for product A', 19.99, 100, 'Electronics', toTimestamp(now()), toTimestamp(now()), true);

INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'Test Product B', 'Description for product B', 29.99, 50, 'Books', toTimestamp(now()), toTimestamp(now()), true);

INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
VALUES (cccccccc-cccc-cccc-cccc-cccccccccccc, 'Test Product C', 'Description for product C', 39.99, 75, 'Clothing', toTimestamp(now()), toTimestamp(now()), true);

INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
VALUES (dddddddd-dddd-dddd-dddd-dddddddddddd, 'Test Product D', 'Description for product D', 49.99, 25, 'Electronics', toTimestamp(now()), toTimestamp(now()), false);

INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
VALUES (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 'Test Product E', 'Description for product E', 59.99, 10, 'Home', toTimestamp(now()), toTimestamp(now()), true);

-- Orders table test data
INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
VALUES (f0000000-0000-0000-0000-000000000001, 11111111-1111-1111-1111-111111111111, toTimestamp(now()), 99.95, 'pending', '123 Test St, City, ST 12345', toTimestamp(now()), toTimestamp(now()));

INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
VALUES (f0000000-0000-0000-0000-000000000002, 22222222-2222-2222-2222-222222222222, toTimestamp(now()), 149.90, 'processing', '456 Test Ave, City, ST 12345', toTimestamp(now()), toTimestamp(now()));

INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
VALUES (f0000000-0000-0000-0000-000000000003, 11111111-1111-1111-1111-111111111111, toTimestamp(now()), 79.98, 'shipped', '123 Test St, City, ST 12345', toTimestamp(now()), toTimestamp(now()));

INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
VALUES (f0000000-0000-0000-0000-000000000004, 33333333-3333-3333-3333-333333333333, toTimestamp(now()), 199.99, 'delivered', '789 Test Blvd, City, ST 12345', toTimestamp(now()), toTimestamp(now()));

INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
VALUES (f0000000-0000-0000-0000-000000000005, 44444444-4444-4444-4444-444444444444, toTimestamp(now()), 59.99, 'cancelled', '321 Test Ln, City, ST 12345', toTimestamp(now()), toTimestamp(now()));

-- Order Items table test data
INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000001, f0000000-0000-0000-0000-000000000001, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 2, 19.99, 39.98, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000002, f0000000-0000-0000-0000-000000000001, bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 2, 29.99, 59.98, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000003, f0000000-0000-0000-0000-000000000002, cccccccc-cccc-cccc-cccc-cccccccccccc, 1, 39.99, 39.99, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000004, f0000000-0000-0000-0000-000000000002, dddddddd-dddd-dddd-dddd-dddddddddddd, 2, 49.99, 99.98, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000005, f0000000-0000-0000-0000-000000000003, eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 1, 59.99, 59.99, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000006, f0000000-0000-0000-0000-000000000003, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 1, 19.99, 19.99, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000007, f0000000-0000-0000-0000-000000000004, bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 5, 29.99, 149.95, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000008, f0000000-0000-0000-0000-000000000004, cccccccc-cccc-cccc-cccc-cccccccccccc, 1, 39.99, 39.99, toTimestamp(now()));

INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
VALUES (e0000000-0000-0000-0000-000000000009, f0000000-0000-0000-0000-000000000005, eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 1, 59.99, 59.99, toTimestamp(now()));

-- Inventory Transactions table test data
INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
VALUES (d0000000-0000-0000-0000-000000000001, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'sale', -2, toTimestamp(now()), 'Order f0000000-0000-0000-0000-000000000001', toTimestamp(now()));

INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
VALUES (d0000000-0000-0000-0000-000000000002, bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'sale', -2, toTimestamp(now()), 'Order f0000000-0000-0000-0000-000000000001', toTimestamp(now()));

INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
VALUES (d0000000-0000-0000-0000-000000000003, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'restock', 50, toTimestamp(now()), 'Weekly restock', toTimestamp(now()));

INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
VALUES (d0000000-0000-0000-0000-000000000004, cccccccc-cccc-cccc-cccc-cccccccccccc, 'adjustment', -5, toTimestamp(now()), 'Damaged inventory', toTimestamp(now()));

INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
VALUES (d0000000-0000-0000-0000-000000000005, eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 'sale', -1, toTimestamp(now()), 'Order f0000000-0000-0000-0000-000000000003', toTimestamp(now()));
