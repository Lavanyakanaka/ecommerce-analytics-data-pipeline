-- Create Staging Schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging Customers Table
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging Products Table
CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    brand VARCHAR(100),
    stock_quantity INTEGER,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging Transactions Table
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address VARCHAR(500),
    total_amount DECIMAL(12, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging Transaction
