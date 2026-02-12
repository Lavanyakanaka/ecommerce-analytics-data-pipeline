import pandas as pd
import streamlit as st

from scripts.db_connection import get_engine


engine = get_engine()

st.title("🚀 E-Commerce Analytics Dashboard")

# Metric 1: Total Revenue
rev_df = pd.read_sql("SELECT SUM(total_sales) FROM warehouse.fact_sales", engine)
st.metric("Total Revenue", f"${rev_df.iloc[0,0]:,.2f}")

# Chart 1: Sales by Category
cat_df = pd.read_sql("""
    SELECT p.category, SUM(f.total_sales) as revenue 
    FROM warehouse.fact_sales f 
    JOIN warehouse.dim_products p ON f.product_key = p.product_key 
    GROUP BY 1""", engine)
st.bar_chart(cat_df.set_index('category'))