## Dashboard Guide

### Data Sources
Connect to PostgreSQL and use the `warehouse` schema for analytics-ready data.

Recommended tables:
- `warehouse.fact_sales`
- `warehouse.dim_customers`
- `warehouse.dim_products`
- `warehouse.dim_date`
- `warehouse.agg_sales_daily`
- `warehouse.agg_sales_monthly`
- `warehouse.agg_sales_category`

### Power BI Steps
1. Get Data -> PostgreSQL.
2. Use the database credentials from `.env`.
3. Load warehouse tables listed above.
4. Create measures for revenue, orders, and average order value.
5. Save the report in `dashboards/powerbi/`.

### Tableau Steps
1. Connect to PostgreSQL.
2. Select the warehouse tables.
3. Build dashboards for revenue trends and category performance.
4. Save the workbook in `dashboards/tableau/`.

### Sample Metrics
- Total revenue
- Orders per month
- Revenue by category
- Top products by revenue
