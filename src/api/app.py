from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from scripts.db_connection import get_engine


app = FastAPI(title="Ecommerce Analytics API", version="1.0.0")
engine = get_engine()


def _fetch_all(query: str, params: dict | None = None) -> list[dict]:
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row) for row in result.mappings().all()]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/analytics/top-products")
def top_products(limit: int = 10) -> list[dict]:
    query = """
        SELECT p.product_name, p.category, SUM(f.total_sales) AS total_revenue
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_products p ON f.product_key = p.product_key
        GROUP BY p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT :limit
    """
    return _fetch_all(query, {"limit": limit})


@app.get("/analytics/monthly-trend")
def monthly_trend() -> list[dict]:
    query = """
        SELECT d.year, d.month, SUM(f.total_sales) AS revenue
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """
    return _fetch_all(query)


@app.get("/analytics/category-summary")
def category_summary() -> list[dict]:
    query = """
        SELECT category, total_orders, total_quantity, total_sales
        FROM warehouse.agg_sales_category
        ORDER BY total_sales DESC
    """
    return _fetch_all(query)


@app.get("/analytics/summary")
def sales_summary() -> list[dict]:
    query = """
        SELECT
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_quantity,
            SUM(total_sales) AS total_revenue
        FROM warehouse.fact_sales
    """
    return _fetch_all(query)
