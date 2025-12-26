"""
Data Ingestion Script - Loads CSV files to PostgreSQL staging schema
"""

import pandas as pd
import logging
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml
import json
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self):
        self.config = self.load_config()
        self.db_config = self.config['database']
        self.engine = self.create_engine()
        
    def load_config(self):
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    def create_engine(self):
        """Create SQLAlchemy engine"""
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}"
            f"/{self.db_config['database']}"
        )
        return create_engine(connection_string)
    
    def create_schemas(self):
        """Create database schemas"""
        logger.info("Creating database schemas...")
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS production"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
                conn.commit()
            logger.info("✓ Schemas created")
        except Exception as e:
            logger.error(f"Error creating schemas: {str(e)}")
            raise
    
    def load_csv_to_staging(self, csv_path: str, table_name: str) -> dict:
        """Load CSV file to staging table"""
        logger.info(f"Loading {table_name} from {csv_path}...")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            
            # Truncate table first
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE staging.{table_name}"))
                conn.commit()
            
            # Load data
            df.to_sql(table_name, self.engine, schema='staging', if_exists='append', index=False)
            
            logger.info(f"✓ Loaded {len(df)} records into staging.{table_name}")
            
            return {
                'table': table_name,
                'rows_loaded': len(df),
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
            return {
                'table': table_name,
                'rows_loaded': 0,
                'status': 'failed',
                'error': str(e)
            }
    
    def validate_staging_load(self) -> dict:
        """Validate staging tables"""
        logger.info("Validating staging load...")
        
        validation = {}
        tables = ['customers', 'products', 'transactions', 'transaction_items']
        
        try:
            with self.engine.connect() as conn:
                for table in tables:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM staging.{table}")
                    ).scalar()
                    validation[table] = result
                    logger.info(f"  staging.{table}: {result} records")
            
            return {
                'status': 'success',
                'tables': validation
            }
            
        except Exception as e:
            logger.error(f"Error validating load: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def generate_ingestion_report(self, results: list):
        """Generate ingestion summary report"""
        logger.info("Generating ingestion report...")
        
        os.makedirs('data/staging', exist_ok=True)
        
        report = {
            'ingestion_timestamp': datetime.now().isoformat(),
            'tables_loaded': {
                result['table']: {
                    'rows_loaded': result['rows_loaded'],
                    'status': result['status']
                } for result in results
            }
        }
        
        with open('data/staging/ingestion_summary.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("✓ Report saved to data/staging/ingestion_summary.json")
        return report

def main():
    logger.info("=" * 50)
    logger.info("Starting Data Ingestion")
    logger.info("=" * 50)
    
    try:
        ingestion = DataIngestion()
        
        # Create schemas
        ingestion.create_schemas()
        
        # Load data
        results = []
        csv_files = {
            'data/raw/customers.csv': 'customers',
            'data/raw/products.csv': 'products',
            'data/raw/transactions.csv': 'transactions',
            'data/raw/transaction_items.csv': 'transaction_items'
        }
        
        for csv_path, table_name in csv_files.items():
            if os.path.exists(csv_path):
                result = ingestion.load_csv_to_staging(csv_path, table_name)
                results.append(result)
            else:
                logger.warning(f"File not found: {csv_path}")
        
        # Validate
        validation = ingestion.validate_staging_load()
        
        # Generate report
        ingestion.generate_ingestion_report(results)
        
        logger.info("=" * 50)
        logger.info("✓ Data Ingestion Complete!")
        logger.info("=" * 50)
        
        return True
