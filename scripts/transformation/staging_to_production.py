"""
Transform and load data from Staging to Production
"""

import pandas as pd
import logging
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml
from datetime import datetime
import re

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StagingToProductionTransform:
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
    
    def cleanse_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse customer data"""
        logger.info("Cleansing customer data...")
        
        df = df.copy()
        
        # Trim whitespace
        df['first_name'] = df['first_name'].str.strip()
        df['last_name'] = df['last_name'].str.strip()
        
        # Lowercase email
        df['email'] = df['email'].str.lower().str.strip()
        
        # Remove nulls
        df = df.dropna(subset=['customer_id', 'email'])
        
        logger.info(f"✓ Cleansed {len(df)} customer records")
        return df
    
    def cleanse_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleanse product data"""
        logger.info("Cleansing product data...")
        
        df = df.copy()
        
        # Trim whitespace
        df['product_name'] = df['product_name'].str.strip()
        df['category'] = df['category'].str.strip()
        df['brand'] = df['brand'].str.strip()
        
        # Remove nulls
        df = df.dropna(subset=['product_id', 'price', 'cost'])
        
        # Ensure cost < price
        df = df[df['cost'] < df['price']]
        
        logger.info(f"✓ Cleansed {len(df)} product records")
        return df
    
    def apply_business_rules(self, df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
        """Apply business rules"""
        logger.info(f"Applying business rules ({rule_type})...")
        
        df = df.copy()
        
        if rule_type == 'products':
            # Add profit margin
            df['profit_margin'] = ((df['price'] - df['cost']) / df['price'] * 100).round(2)
            
            # Add price category
            df['price_range'] = pd.cut(
                df['price'],
                bins=[0, 50, 200, float('inf')],
                labels=['Budget', 'Mid-range', 'Premium']
            )
        
        logger.info(f"✓ Applied business rules to {len(df)} records")
        return df
    
    def load_to_production(self, df: pd.DataFrame, table_name: str, strategy: str = 'truncate') -> dict:
        """Load data to production table"""
        logger.info(f"Loading {len(df)} records to production.{table_name} (strategy: {strategy})...")
        
        try:
            if strategy == 'truncate':
                # Truncate and reload
                with self.engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE production.{table_name}"))
                    conn.commit()
            
            # Load data
            df.to_sql(table_name, self.engine, schema='production', if_exists='append', index=False)
            
            logger.info(f"✓ Loaded {len(df)} records to production.{table_name}")
            
            return {
                'table': table_name,
                'records_loaded': len(df),
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error loading to production: {str(e)}")
            return {
                'table': table_name,
                'records_loaded': 0,
                'status': 'failed',
                'error': str(e)
            }

def main():
    logger.info("=" * 50)
    logger.info("Starting Staging to Production Transformation")
    logger.info("=" * 50)
    
    try:
        transform = StagingToProductionTransform()
        results = []
        
        #
