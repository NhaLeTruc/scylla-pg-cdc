-- Handle Partial UPDATE from ScyllaDB CDC
-- ScyllaDB CDC only sends changed columns, causing NULLs in unchanged fields
-- This trigger preserves existing values when NULL is provided in an UPDATE

-- Function to handle partial updates for users table
CREATE OR REPLACE FUNCTION cdc_data.preserve_existing_values_users()
RETURNS TRIGGER AS $$
BEGIN
    -- Only apply for UPDATEs, not INSERTs
    IF TG_OP = 'UPDATE' THEN
        -- Preserve username if new value is NULL
        IF NEW.username IS NULL AND OLD.username IS NOT NULL THEN
            NEW.username := OLD.username;
        END IF;
        
        -- Preserve email if new value is NULL
        IF NEW.email IS NULL AND OLD.email IS NOT NULL THEN
            NEW.email := OLD.email;
        END IF;
        
        -- Preserve first_name if new value is NULL
        IF NEW.first_name IS NULL AND OLD.first_name IS NOT NULL THEN
            NEW.first_name := OLD.first_name;
        END IF;
        
        -- Preserve last_name if new value is NULL
        IF NEW.last_name IS NULL AND OLD.last_name IS NOT NULL THEN
            NEW.last_name := OLD.last_name;
        END IF;
        
        -- Preserve created_at if new value is NULL
        IF NEW.created_at IS NULL AND OLD.created_at IS NOT NULL THEN
            NEW.created_at := OLD.created_at;
        END IF;
        
        -- Only update updated_at if a real change occurred
        -- Don't preserve updated_at - let it be updated
        
        -- Preserve metadata if new value is NULL
        IF NEW.metadata IS NULL AND OLD.metadata IS NOT NULL THEN
            NEW.metadata := OLD.metadata;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for users table
DROP TRIGGER IF EXISTS preserve_users_values ON cdc_data.users;
CREATE TRIGGER preserve_users_values
    BEFORE UPDATE ON cdc_data.users
    FOR EACH ROW
    EXECUTE FUNCTION cdc_data.preserve_existing_values_users();

-- Function to handle partial updates for products table
CREATE OR REPLACE FUNCTION cdc_data.preserve_existing_values_products()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.sku IS NULL AND OLD.sku IS NOT NULL THEN
            NEW.sku := OLD.sku;
        END IF;
        IF NEW.name IS NULL AND OLD.name IS NOT NULL THEN
            NEW.name := OLD.name;
        END IF;
        IF NEW.description IS NULL AND OLD.description IS NOT NULL THEN
            NEW.description := OLD.description;
        END IF;
        IF NEW.category IS NULL AND OLD.category IS NOT NULL THEN
            NEW.category := OLD.category;
        END IF;
        IF NEW.price IS NULL AND OLD.price IS NOT NULL THEN
            NEW.price := OLD.price;
        END IF;
        IF NEW.currency IS NULL AND OLD.currency IS NOT NULL THEN
            NEW.currency := OLD.currency;
        END IF;
        IF NEW.stock_quantity IS NULL AND OLD.stock_quantity IS NOT NULL THEN
            NEW.stock_quantity := OLD.stock_quantity;
        END IF;
        IF NEW.is_active IS NULL AND OLD.is_active IS NOT NULL THEN
            NEW.is_active := OLD.is_active;
        END IF;
        IF NEW.created_at IS NULL AND OLD.created_at IS NOT NULL THEN
            NEW.created_at := OLD.created_at;
        END IF;
        IF NEW.attributes IS NULL AND OLD.attributes IS NOT NULL THEN
            NEW.attributes := OLD.attributes;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for products table
DROP TRIGGER IF EXISTS preserve_products_values ON cdc_data.products;
CREATE TRIGGER preserve_products_values
    BEFORE UPDATE ON cdc_data.products
    FOR EACH ROW
    EXECUTE FUNCTION cdc_data.preserve_existing_values_products();

-- Function to handle partial updates for orders table
CREATE OR REPLACE FUNCTION cdc_data.preserve_existing_values_orders()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.user_id IS NULL AND OLD.user_id IS NOT NULL THEN
            NEW.user_id := OLD.user_id;
        END IF;
        IF NEW.order_number IS NULL AND OLD.order_number IS NOT NULL THEN
            NEW.order_number := OLD.order_number;
        END IF;
        IF NEW.order_date IS NULL AND OLD.order_date IS NOT NULL THEN
            NEW.order_date := OLD.order_date;
        END IF;
        IF NEW.total_amount IS NULL AND OLD.total_amount IS NOT NULL THEN
            NEW.total_amount := OLD.total_amount;
        END IF;
        IF NEW.currency IS NULL AND OLD.currency IS NOT NULL THEN
            NEW.currency := OLD.currency;
        END IF;
        IF NEW.shipping_address IS NULL AND OLD.shipping_address IS NOT NULL THEN
            NEW.shipping_address := OLD.shipping_address;
        END IF;
        IF NEW.billing_address IS NULL AND OLD.billing_address IS NOT NULL THEN
            NEW.billing_address := OLD.billing_address;
        END IF;
        IF NEW.created_at IS NULL AND OLD.created_at IS NOT NULL THEN
            NEW.created_at := OLD.created_at;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for orders table
DROP TRIGGER IF EXISTS preserve_orders_values ON cdc_data.orders;
CREATE TRIGGER preserve_orders_values
    BEFORE UPDATE ON cdc_data.orders
    FOR EACH ROW
    EXECUTE FUNCTION cdc_data.preserve_existing_values_orders();

COMMENT ON FUNCTION cdc_data.preserve_existing_values_users() IS 
'Preserves existing column values when NULL is provided in UPDATEs from ScyllaDB CDC partial updates';

COMMENT ON FUNCTION cdc_data.preserve_existing_values_products() IS 
'Preserves existing column values when NULL is provided in UPDATEs from ScyllaDB CDC partial updates';

COMMENT ON FUNCTION cdc_data.preserve_existing_values_orders() IS 
'Preserves existing column values when NULL is provided in UPDATEs from ScyllaDB CDC partial updates';
