-- Create the Warehouse Schema
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 1. Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key DATE PRIMARY KEY,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER,
    is_weekend BOOLEAN
);

-- 2. Customer Dimension (Denormalized)
CREATE TABLE IF NOT EXISTS warehouse.dim_customers AS 
SELECT 
    customer_id,
    first_name || ' ' || last_name AS full_name,
    email,
    city,
    state,
    age_group,
    registration_date
FROM production.customers;

-- 3. Product Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_products AS 
SELECT 
    product_id,
    product_name,
    category,
    brand,
    price
FROM production.products;

-- 4. Sales Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(20),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    date_key DATE,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_sales DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    gross_total DECIMAL(12,2)
);