-- Query 1: Top 10 Products by Revenue
SELECT 
    dp.product_name,
    dp.category,
    SUM(fs.line_total) as total_revenue,
    SUM(fs.quantity) as units_sold,
    AVG(fs.unit_price) as avg_price
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_key, dp.product_name, dp.category
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
SELECT 
    dd.year,
    dd.month,
    CONCAT(dd.month_name, ' ', dd.year) as year_month,
    SUM(fs.line_total) as total_revenue,
    COUNT(DISTINCT fs.transaction_id) as total_transactions,
    AVG(fs.line_total) as average_order_value,
    COUNT(DISTINCT fs.customer_key) as unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Query 3: Customer Segmentation
WITH customer_totals AS (
    SELECT 
        fs.customer_key,
        SUM(fs.line_total) as total_spent
    FROM warehouse.fact_sales fs
    GROUP BY fs.customer_key
)
SELECT 
    CASE 
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END as spending_segment,
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue,
    AVG(total_spent) as avg_transaction_value
FROM customer_totals
GROUP BY spending_segment
ORDER BY AVG(total_spent) DESC;

-- Query 4: Category Performance
SELECT 
    dp.category,
    SUM(fs.line_total) as total_revenue,
    SUM(fs.profit) as total_profit,
    ROUND(100.0 * SUM(fs.profit) / SUM(fs.line_total), 2) as profit_margin_pct,
    SUM(fs.quantity) as units_sold
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.category
ORDER BY total_revenue DESC;

-- Query 5: Payment Method Distribution
SELECT 
    dpm.payment_method_name,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.line_total) as total_revenue,
    ROUND(100.0 * COUNT(DISTINCT fs.transaction_id) / 
        (SELECT COUNT(DISTINCT transaction_id) FROM warehouse.fact_sales), 2) as pct_of_transactions
FROM warehouse.fact_sales fs
JOIN warehouse.dim_payment_method dpm ON fs.payment_method_key = dpm.payment_method_key
GROUP BY dpm.payment_method_key, dpm.payment_method_name
ORDER BY transaction_count DESC;

-- Query 6: Geographic Analysis
SELECT 
    dc.state,
    SUM(fs.line_total) as total_revenue,
    COUNT(DISTINCT fs.customer_key) as total_customers,
    ROUND(SUM(fs.line_total) / COUNT(DISTINCT fs.customer_key), 2) as avg_revenue_per_customer
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.state
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 7: Customer Lifetime Value
SELECT 
    dc.customer_id,
    dc.full_name,
    SUM(fs.line_total) as total_spent,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    EXTRACT(DAY FROM CURRENT_DATE - dc.registration_date) as days_since_registration,
    ROUND(AVG(fs.line_total), 2) as avg_order_value
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_key, dc.customer_id, dc.full_name, dc.registration_date
ORDER BY total_spent DESC
LIMIT 20;

-- Query 8: Product Profitability
SELECT 
    dp.product_name,
    dp.category,
    SUM(fs.profit) as total_profit,
    ROUND(100.0 * SUM(fs.profit) / SUM(fs.line_total), 2) as profit_margin_pct,
    SUM(fs.line_total) as revenue,
    SUM(fs.quantity) as units_sold
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_key, dp.product_name, dp.category
ORDER BY total_profit DESC
LIMIT 15;

-- Query 9: Day of Week Sales Pattern
SELECT 
    dd.day_name,
    ROUND(AVG(fs.line_total), 2) as avg_daily_revenue,
    COUNT(DISTINCT fs.transaction_id) / COUNT(DISTINCT dd.date_key) as avg_daily_transactions,
    SUM(fs.line_total) as total_revenue
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.day_name
ORDER BY CASE 
    WHEN dd.day_name = 'Monday' THEN 1
    WHEN dd.day_name = 'Tuesday' THEN 2
    WHEN dd.day_name = 'Wednesday' THEN 3
    WHEN dd.day_name = 'Thursday' THEN 4
    WHEN dd.day_name = 'Friday' THEN 5
    WHEN dd.day_name = 'Saturday' THEN 6
    WHEN dd.day_name = 'Sunday' THEN 7
END;

-- Query 10: Discount Impact Analysis
SELECT 
    CASE 
        WHEN fs.discount_amount = 0 THEN '0%'
        WHEN fs.discount_amount / (fs.unit_price * fs.quantity) * 100 <= 10 THEN '1-10%'
        WHEN fs.discount_amount / (fs.unit_price * fs.quantity) * 100 <= 25 THEN '11-25%'
        WHEN fs.discount_amount / (fs.unit_price * fs.quantity) * 100 <= 50 THEN '26-50%'
        ELSE '50%+'
    END as discount_range,
    AVG(fs.discount_amount / (fs.unit_price * fs.quantity) * 100) as avg_discount_pct,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.line_total) as total_revenue,
    AVG(fs.line_total) as avg_line_total
FROM warehouse.fact_sales fs
GROUP BY discount_range
ORDER BY avg_discount_pct;
