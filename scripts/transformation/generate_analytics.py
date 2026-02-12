import json
import os
import time
from datetime import datetime

import pandas as pd

from scripts.db_connection import get_connection

def execute_and_export():
    output_dir = 'data/processed/analytics/'
    os.makedirs(output_dir, exist_ok=True)
    
    # Define your queries in a dictionary
    queries = {
        "query1_top_products": """
            SELECT p.product_name, p.category, SUM(f.total_sales) as total_revenue 
            FROM warehouse.fact_sales f 
            JOIN warehouse.dim_products p ON f.product_key = p.product_key 
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10""",
        "query2_monthly_trend": """
            SELECT d.year, d.month, SUM(f.total_sales) as revenue 
            FROM warehouse.fact_sales f 
            JOIN warehouse.dim_date d ON f.date_key = d.date_key 
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "query5_payment_distribution": """
            SELECT payment_method, COUNT(*) as txn_count, SUM(total_amount) as revenue 
            FROM production.transactions GROUP BY 1"""
    }
    
    summary = {
        "generation_timestamp": datetime.now().isoformat(),
        "queries_executed": len(queries),
        "query_results": {}
    }

    start_time = time.time()
    conn = get_connection()

    for name, sql in queries.items():
        q_start = time.time()
        df = pd.read_sql(sql, conn)
        df.to_csv(f"{output_dir}{name}.csv", index=False)
        
        summary["query_results"][name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": round((time.time() - q_start) * 1000, 2)
        }
    
    conn.close()
    summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)
    
    with open('data/processed/analytics/analytics_summary.json', 'w') as f:
        json.dump(summary, f, indent=4)
    
    print("✅ Analytics Exported to data/processed/analytics/")

if __name__ == "__main__":
    execute_and_export()