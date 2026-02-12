-- Monitoring queries for pipeline health

-- Row counts by schema
SELECT 'staging.customers' AS table_name, COUNT(*) AS row_count FROM staging.customers
UNION ALL
SELECT 'staging.products', COUNT(*) FROM staging.products
UNION ALL
SELECT 'staging.transactions', COUNT(*) FROM staging.transactions
UNION ALL
SELECT 'staging.transaction_items', COUNT(*) FROM staging.transaction_items;

SELECT 'production.customers' AS table_name, COUNT(*) AS row_count FROM production.customers
UNION ALL
SELECT 'production.products', COUNT(*) FROM production.products
UNION ALL
SELECT 'production.transactions', COUNT(*) FROM production.transactions
UNION ALL
SELECT 'production.transaction_items', COUNT(*) FROM production.transaction_items;

SELECT 'warehouse.dim_customers' AS table_name, COUNT(*) AS row_count FROM warehouse.dim_customers
UNION ALL
SELECT 'warehouse.dim_products', COUNT(*) FROM warehouse.dim_products
UNION ALL
SELECT 'warehouse.fact_sales', COUNT(*) FROM warehouse.fact_sales;

-- Latest transaction date
SELECT MAX(transaction_date) AS latest_transaction_date
FROM production.transactions;

-- Aggregate tables freshness
SELECT MAX(date_key) AS latest_agg_date
FROM warehouse.agg_sales_daily;
