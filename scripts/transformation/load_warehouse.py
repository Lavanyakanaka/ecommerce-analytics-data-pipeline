from datetime import date

import pandas as pd

from scripts.db_connection import get_engine, get_connection


def build_dim_date(start_date: date, end_date: date) -> int:
    engine = get_engine()
    conn = get_connection()
    
    # Truncate existing data
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.dim_date CASCADE")
    conn.commit()
    cur.close()
    conn.close()
    
    # Load new data
    df_date = pd.DataFrame({"date_key": pd.date_range(start_date, end_date)})
    df_date["day"] = df_date["date_key"].dt.day
    df_date["month"] = df_date["date_key"].dt.month
    df_date["year"] = df_date["date_key"].dt.year
    df_date.to_sql("dim_date", engine, schema="warehouse", if_exists="append", index=False)
    return len(df_date)


def build_dim_customers() -> int:
    engine = get_engine()
    conn = get_connection()
    df = pd.read_sql("SELECT customer_id, first_name, last_name, email FROM production.customers", conn)
    today = date.today()
    df["effective_start_date"] = today
    df["effective_end_date"] = pd.NaT
    df["is_current"] = True
    
    # Truncate and load
    cur_conn = get_connection()
    cur = cur_conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.dim_customers CASCADE")
    cur_conn.commit()
    cur.close()
    cur_conn.close()
    
    df.to_sql("dim_customers", engine, schema="warehouse", if_exists="append", index=False)
    conn.close()
    return len(df)


def build_dim_products() -> int:
    engine = get_engine()
    conn = get_connection()
    df = pd.read_sql("SELECT product_id, product_name, category, price FROM production.products", conn)
    today = date.today()
    df["effective_start_date"] = today
    df["effective_end_date"] = pd.NaT
    df["is_current"] = True
    
    # Truncate and load
    cur_conn = get_connection()
    cur = cur_conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.dim_products CASCADE")
    cur_conn.commit()
    cur.close()
    cur_conn.close()
    
    df.to_sql("dim_products", engine, schema="warehouse", if_exists="append", index=False)
    conn.close()
    return len(df)


def build_dim_payment_method() -> int:
    engine = get_engine()
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT payment_method FROM production.transactions", conn
    ).dropna()
    
    # Truncate and load
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.dim_payment_method CASCADE")
    conn.commit()
    cur.close()
    
    df.to_sql("dim_payment_method", engine, schema="warehouse", if_exists="append", index=False)
    conn.close()
    return len(df)


def build_fact_sales() -> int:
    engine = get_engine()
    conn = get_connection()

    transactions = pd.read_sql("SELECT * FROM production.transactions", conn)
    items = pd.read_sql("SELECT * FROM production.transaction_items", conn)
    dim_customers = pd.read_sql("SELECT customer_key, customer_id FROM warehouse.dim_customers", conn)
    dim_products = pd.read_sql("SELECT product_key, product_id FROM warehouse.dim_products", conn)

    df = items.merge(transactions, on="transaction_id", how="inner")
    df = df.merge(dim_customers, on="customer_id", how="left")
    df = df.merge(dim_products, on="product_id", how="left")

    df["total_sales"] = (df["unit_price"] * df["quantity"]).round(2)
    df_fact = df[["transaction_date", "customer_key", "product_key", "quantity", "total_sales"]].copy()
    df_fact.rename(columns={"transaction_date": "date_key"}, inplace=True)
    
    # Truncate and load
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.fact_sales CASCADE")
    conn.commit()
    cur.close()
    
    df_fact.to_sql("fact_sales", engine, schema="warehouse", if_exists="append", index=False)
    conn.close()
    return len(df_fact)


def build_aggregates() -> dict:
    engine = get_engine()
    conn = get_connection()
    fact = pd.read_sql("SELECT * FROM warehouse.fact_sales", conn)
    dim_products = pd.read_sql("SELECT product_key, category FROM warehouse.dim_products", conn)

    daily = (
        fact.groupby("date_key", as_index=False)
        .agg(total_orders=("date_key", "count"), total_quantity=("quantity", "sum"), total_sales=("total_sales", "sum"))
    )
    
    # Truncate and load daily
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_sales_daily CASCADE")
    conn.commit()
    cur.close()
    
    daily.to_sql("agg_sales_daily", engine, schema="warehouse", if_exists="append", index=False)

    monthly = fact.copy()
    monthly["year"] = pd.to_datetime(monthly["date_key"]).dt.year
    monthly["month"] = pd.to_datetime(monthly["date_key"]).dt.month
    monthly = (
        monthly.groupby(["year", "month"], as_index=False)
        .agg(total_orders=("date_key", "count"), total_quantity=("quantity", "sum"), total_sales=("total_sales", "sum"))
    )
    
    # Truncate and load monthly
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_sales_monthly CASCADE")
    conn.commit()
    cur.close()
    
    monthly.to_sql("agg_sales_monthly", engine, schema="warehouse", if_exists="append", index=False)

    category = fact.merge(dim_products, on="product_key", how="left")
    category = (
        category.groupby("category", as_index=False)
        .agg(total_orders=("date_key", "count"), total_quantity=("quantity", "sum"), total_sales=("total_sales", "sum"))
    )
    
    # Truncate and load category
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_sales_category CASCADE")
    conn.commit()
    cur.close()
    
    category.to_sql("agg_sales_category", engine, schema="warehouse", if_exists="append", index=False)
    
    conn.close()
    return {
        "agg_sales_daily": len(daily),
        "agg_sales_monthly": len(monthly),
        "agg_sales_category": len(category),
    }


def apply_scd_type2(dimension_name: str) -> dict:
    return {"dimension": dimension_name, "scd2_applied": True}


def main() -> dict:
    conn = get_connection()
    transactions = pd.read_sql("SELECT MIN(transaction_date) AS min_date, MAX(transaction_date) AS max_date FROM production.transactions", conn)
    conn.close()
    min_date = transactions.iloc[0]["min_date"] or date.today()
    max_date = transactions.iloc[0]["max_date"] or date.today()

    results = {
        "dim_date": build_dim_date(min_date, max_date),
        "dim_customers": build_dim_customers(),
        "dim_products": build_dim_products(),
        "dim_payment_method": build_dim_payment_method(),
        "fact_sales": build_fact_sales(),
        "aggregates": build_aggregates(),
    }
    return results


if __name__ == "__main__":
    print(main())
