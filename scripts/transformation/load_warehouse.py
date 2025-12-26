"""
Load data to Warehouse Schema (Star Schema)
"""

import logging
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml
from datetime import datetime, timedelta
import pandas as pd

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/warehouse_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WarehouseLoader:
    def __init__(self):
        self.config = self.load_config()
        self.db_config = self.config['database']
        self.engine = self.create_engine()
    
    def load_config(self):
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    def create_engine(self):
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}"
            f"/{self.db_config['database']}"
        )
        return create_engine(connection_string)
    
    def build_dim_date(self, start_date: str, end_date: str) -> int:
        """Build date dimension"""
        logger.info(f"Building dim_date from {start_date} to {end_date}...")
        
        from datetime import datetime, timedelta
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        while current <= end:
            date_key = int(current.strftime('%Y%m%d'))
            dates.append({
                'date_key': date_key,
                'full_date': current.date(),
                'year': current.year,
                'quarter': (current.month - 1) // 3 + 1,
                'month': current.month,
                'day': current.day,
                'month_name': current.strftime('%B'),
                'day_name': current.strftime('%A'),
                'week_of_year': current.isocalendar()[1],
                'is_weekend': current.weekday() >= 5
            })
            current += timedelta(days=1)
        
        df = pd.DataFrame(dates)
        
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE warehouse.dim_date"))
            conn.commit()
        
        df.to_sql('dim_date', self.engine, schema='warehouse', if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(dates)} date records")
        return len(dates)
    
    def build_dim_payment_method(self) -> int:
        """Build payment method dimension"""
        logger.info("Building dim_payment_method...")
        
        payment_methods = [
            {'payment_method_name': 'Credit Card', 'payment_type': 'Online'},
            {'payment_method_name': 'Debit Card', 'payment_type': 'Online'},
            {'payment_method_name': 'UPI', 'payment_type': 'Online'},
            {'payment_method_name': 'Net Banking', 'payment_type': 'Online'},
            {'payment_method_name': 'Cash on Delivery', 'payment_type': 'Offline'}
        ]
        
        df = pd.DataFrame(payment_methods)
        
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE warehouse.dim_payment_method"))
            conn.commit()
        
        df.to_sql('dim_payment_method', self.engine, schema='warehouse', if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(payment_methods)} payment methods")
        return len(payment_methods)
    
    def build_dim_customers(self) -> int:
        """Build customer dimension (SCD Type 2)"""
        logger.info("Building dim_customers with SCD Type 2...")
        
        # Get customers from production
        df = pd.read_sql(
            "SELECT * FROM production.customers ORDER BY customer_id",
            self.engine
        )
        
        # Add SCD Type 2 columns
        df['effective_date'] = pd.to_datetime('today').date()
        df['end_date'] = None
        df['is_current'] = True
        
        # Add full_name
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        
        # Add customer segment (simplified)
        df['customer_segment'] = 'Regular'
        
        # Select required columns
        columns = [
            'customer_id', 'full_name', 'email', 'city', 'state', 'country',
            'age_group', 'customer_segment', 'registration_date',
            'effective_date', 'end_date', 'is_current'
        ]
        df = df[columns]
        
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE warehouse.dim_customers"))
            conn.commit()
        
        df.to_sql('dim_customers', self.engine, schema='warehouse', if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(df)} customer dimensions")
        return len(df)
    
    def build_dim_products(self) -> int:
        """Build product dimension (SCD Type 2)"""
        logger.info("Building dim_products with SCD Type 2...")
        
        df = pd.read_sql(
            "SELECT * FROM production.products ORDER BY product_id",
            self.engine
        )
        
        # Add SCD Type 2 columns
        df['effective_date'] = pd.to_datetime('today').date()
        df['end_date'] = None
        df['is_current'] = True
        
        # Add price_range
        df['price_range'] = pd.cut(
            df['price'],
            bins=[0, 50, 200, float('inf')],
            labels=['Budget', 'Mid-range', 'Premium']
        ).astype(str)
        
        # Select required columns
        columns = [
            'product_id', 'product_name', 'category', 'sub_category', 'brand',
            'price_range', 'effective_date', 'end_date', 'is_current'
        ]
        df = df[columns]
        
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE warehouse.dim_products"))
            conn.commit()
        
        df.to_sql('dim_products', self.engine, schema='warehouse', if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(df)} product dimensions")
        return len(df)
    
    def build_fact_sales(self) -> int:
        """Build fact sales table"""
        logger.info("Building fact_sales...")
        
        # Get transaction items with all required data
        query = """
        SELECT 
            ti.item_id,
            ti.transaction_id,
            t.transaction_date,
            t.customer_id,
            ti.product_id,
            t.payment_method,
            ti.quantity,
            ti.unit_price,
            ROUND(ti.unit_price * ti.quantity * ti.discount_percentage / 100.0, 2) as discount_amount,
            ti.line_total,
            ROUND(ti.line_total - (p.cost * ti.quantity), 2) as profit,
            t.created_at
        FROM production.transaction_items ti
        JOIN production.transactions t ON ti.transaction_id = t.transaction_id
        JOIN production.products p ON ti.product_id = p.product_id
        """
        
        df = pd.read_sql(query, self.engine)
        
        # Lookup dimension keys
        with self.engine.connect() as conn:
            # Date key
            df['date_key'] = df['transaction_date'].apply(
                lambda x: int(pd.to_datetime(x).strftime('%Y%m%d'))
            )
            
            # Customer key
            customer_keys = pd.read_sql(
                "SELECT customer_id, customer_key FROM warehouse.dim_customers WHERE is_current = TRUE",
                self.engine
            ).set_index('customer_id')['customer_key'].to_dict()
            df['customer_key'] = df['customer_id'].map(customer_keys)
            
            # Product key
            product_keys = pd.read_sql(
                "SELECT product_id, product_key FROM warehouse.dim_products WHERE is_current = TRUE",
                self.engine
            ).set_index('product_id')['product_key'].to_dict()
            df['product_key'] = df['product_id'].map(product_keys)
            
            # Payment method key
            payment_keys = pd.read_sql(
                "SELECT payment_method_name, payment_method_key FROM warehouse.dim_payment_method",
                self.engine
            ).set_index('payment_method_name')['payment_method_key'].to_dict()
            df['payment_method_key'] = df['payment_method'].map(payment_keys)
        
        # Select required columns
        columns = [
            'date_key', 'customer_key', 'product_key', 'payment_method_key',
            'transaction_id', 'quantity', 'unit_price', 'discount_amount',
            'line_total', 'profit', 'created_at'
        ]
        df = df[columns].dropna()
        
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE warehouse.fact_sales"))
            conn.commit()
        
        df.to_sql('fact_sales', self.engine, schema='warehouse', if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(df)} fact records")
        return len(df)
    
    def build_aggregates(self):
        """Build aggregate tables"""
        logger.info("Building aggregate tables...")
        
        with self.engine.connect() as conn:
            # Daily sales aggregates
            logger.info("Building agg_daily_sales...")
            conn.execute(text("""
                TRUNCATE TABLE warehouse.agg_daily_sales;
                INSERT INTO warehouse.agg_daily_sales
                SELECT 
                    date_key,
                    COUNT(DISTINCT transaction_id) as total_transactions,
                    SUM(line_total) as total_revenue,
                    SUM(profit) as total_profit,
                    COUNT(DISTINCT customer_key) as unique_customers
                FROM warehouse.fact_sales
                GROUP BY date_key
            """))
            
            # Product performance
            logger.info("Building agg_product_performance...")
            conn.execute(text("""
                TRUNCATE TABLE warehouse.agg_product_performance;
                INSERT INTO warehouse.agg_product_performance
                SELECT 
                    product_key,
                    SUM(quantity) as total_quantity_sold,
                    SUM(line_total) as total_revenue,
                    SUM(profit) as total_profit,
                    AVG(discount_amount / (unit_price * quantity) * 100) as avg_discount_percentage
                FROM warehouse.fact_sales
                GROUP BY product_key
            """))
            
            # Customer metrics
            logger.info("Building agg_customer_metrics...")
            conn.execute(text("""
                TRUNCATE TABLE warehouse.agg_customer_metrics;
                INSERT INTO warehouse.agg_customer_metrics
                SELECT 
                    customer_key,
                    COUNT(DISTINCT transaction_id) as total_transactions,
                    SUM(line_total) as total_spent,
                    AVG(line_total) as avg_order_value,
                    MAX(created_at::date) as last_purchase_date
                FROM warehouse.fact_sales
                GROUP BY customer_key
            """))
            
            conn.commit()
        
        logger.info("✓ Aggregates built successfully")

def main():
    logger.info("=" * 50)
    logger.info("Starting Warehouse Load")
    logger.info("=" * 50)
    
    try:
        loader = WarehouseLoader()
        
        # Build dimensions
        loader.build_dim_date('2024-01-01', '2024-12-31')
        loader.build_dim_payment_method()
        loader.build_dim_customers()
        loader.build_dim_products()
        
        # Build fact table
        loader.build_fact_sales()
        
        # Build aggregates
        loader.build_aggregates()
        
        logger.info("=" * 50)
        logger.info("✓ Warehouse Load Complete!")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during warehouse load: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
