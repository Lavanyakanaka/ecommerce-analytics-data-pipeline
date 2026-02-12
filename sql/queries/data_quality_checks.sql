-- Data quality checks for staging and production

-- Null checks (staging)
SELECT 'staging.customers' AS table_name, COUNT(*) AS null_email
FROM staging.customers
WHERE email IS NULL;

SELECT 'staging.products' AS table_name, COUNT(*) AS null_price
FROM staging.products
WHERE price IS NULL;

-- Duplicate checks (production)
SELECT customer_id, COUNT(*) AS duplicate_count
FROM production.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT product_id, COUNT(*) AS duplicate_count
FROM production.products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Referential integrity checks (production)
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
LEFT JOIN production.customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) AS orphan_items
FROM production.transaction_items ti
LEFT JOIN production.transactions t ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

SELECT COUNT(*) AS orphan_products
FROM production.transaction_items ti
LEFT JOIN production.products p ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Range checks
SELECT COUNT(*) AS invalid_total_amount
FROM production.transactions
WHERE total_amount <= 0;

SELECT COUNT(*) AS invalid_quantity
FROM production.transaction_items
WHERE quantity <= 0;
