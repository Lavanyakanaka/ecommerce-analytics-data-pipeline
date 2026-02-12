CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    effective_start_date DATE,
    effective_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INT,
    product_name TEXT,
    category TEXT,
    price NUMERIC(10,2),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key DATE,
    customer_key INT,
    product_key INT,
    quantity INT,
    total_sales NUMERIC(10,2),
    CONSTRAINT fk_fact_sales_date
        FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key),
    CONSTRAINT fk_fact_sales_customer
        FOREIGN KEY (customer_key) REFERENCES warehouse.dim_customers(customer_key),
    CONSTRAINT fk_fact_sales_product
        FOREIGN KEY (product_key) REFERENCES warehouse.dim_products(product_key)
);

CREATE TABLE IF NOT EXISTS warehouse.agg_sales_daily (
    date_key DATE PRIMARY KEY,
    total_orders INT,
    total_quantity INT,
    total_sales NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS warehouse.agg_sales_monthly (
    year INT,
    month INT,
    total_orders INT,
    total_quantity INT,
    total_sales NUMERIC(12,2),
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS warehouse.agg_sales_category (
    category TEXT PRIMARY KEY,
    total_orders INT,
    total_quantity INT,
    total_sales NUMERIC(12,2)
);
