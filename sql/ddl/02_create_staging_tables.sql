CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    gender TEXT,
    signup_date DATE
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    transaction_date DATE,
    payment_method TEXT,
    total_amount NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    transaction_item_id INT PRIMARY KEY,
    transaction_id INT,
    product_id INT,
    quantity INT,
    unit_price NUMERIC(10,2)
);
