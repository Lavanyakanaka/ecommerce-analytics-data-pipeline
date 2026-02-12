-- Query 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products
SELECT 
    p.product_name, 
    p.category, 
    SUM(f.net_sales) AS total_revenue, 
    SUM(f.quantity) AS units_sold, 
    AVG(f.unit_price) AS avg_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
-- Objective: Analyze revenue over time
SELECT 
    d.year || '-' || LPAD(d.month::text, 2, '0') AS year_month,
    SUM(f.net_sales) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.net_sales) / COUNT(DISTINCT f.transaction_id) AS average_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY year_month
ORDER BY year_month;

-- Query 3: Customer Segmentation Analysis
-- Objective: Group customers by spending patterns
WITH CustomerSpend AS (
    SELECT customer_id, SUM(net_sales) as total_spent
    FROM warehouse.fact_sales
    GROUP BY customer_id
)
SELECT 
    CASE 
        WHEN total_spent <= 1000 THEN '$0-$1,000'
        WHEN total_spent <= 5000 THEN '$1,000-$5,000'
        WHEN total_spent <= 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue
FROM CustomerSpend
GROUP BY spending_segment;

-- Query 5: Payment Method Distribution
-- Objective: Understand payment preferences
SELECT 
    payment_method,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    (COUNT(*)::float / SUM(COUNT(*)) OVER()) * 100 as pct_of_transactions
FROM production.transactions
GROUP BY payment_method;