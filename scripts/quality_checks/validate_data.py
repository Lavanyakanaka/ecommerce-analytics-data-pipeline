"""
Data Quality Checks and Validation
"""

import pandas as pd
import logging
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/quality_checks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataQualityValidator:
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
    
    def check_null_values(self, schema: str = 'staging') -> dict:
        """Check for NULL values in mandatory fields"""
        logger.info(f"Checking NULL values in {schema} schema...")
        
        results = {}
        mandatory_columns = {
            'customers': ['customer_id', 'email'],
            'products': ['product_id', 'price', 'cost'],
            'transactions': ['transaction_id', 'customer_id'],
            'transaction_items': ['item_id', 'transaction_id', 'product_id']
        }
        
        try:
            with self.engine.connect() as conn:
                for table, columns in mandatory_columns.items():
                    for col in columns:
                        query = text(
                            f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL"
                        )
                        count = conn.execute(query).scalar()
                        if count > 0:
                            results[f"{table}.{col}"] = count
            
            return {
                'status': 'passed' if not results else 'failed',
                'null_violations': results,
                'total_violations': sum(results.values())
            }
        except Exception as e:
            logger.error(f"Error checking NULLs: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def check_duplicates(self, schema: str = 'staging') -> dict:
        """Check for duplicate primary keys"""
        logger.info(f"Checking duplicates in {schema} schema...")
        
        tables = {
            'customers': 'customer_id',
            'products': 'product_id',
            'transactions': 'transaction_id',
            'transaction_items': 'item_id'
        }
        
        results = {}
        try:
            with self.engine.connect() as conn:
                for table, pk_col in tables.items():
                    query = text(
                        f"SELECT {pk_col}, COUNT(*) as cnt FROM {schema}.{table} "
                        f"GROUP BY {pk_col} HAVING COUNT(*) > 1"
                    )
                    duplicates = conn.execute(query).fetchall()
                    if duplicates:
                        results[table] = len(duplicates)
            
            return {
                'status': 'passed' if not results else 'failed',
                'duplicate_tables': results,
                'total_duplicates': sum(results.values())
            }
        except Exception as e:
            logger.error(f"Error checking duplicates: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def check_referential_integrity(self, schema: str = 'staging') -> dict:
        """Check for orphan records"""
        logger.info(f"Checking referential integrity in {schema} schema...")
        
        results = {}
        try:
            with self.engine.connect() as conn:
                # Check orphan transactions
                query = text(
                    f"SELECT COUNT(*) FROM {schema}.transactions t "
                    f"LEFT JOIN {schema}.customers c ON t.customer_id = c.customer_id "
                    f"WHERE c.customer_id IS NULL"
                )
                orphan_txns = conn.execute(query).scalar()
                if orphan_txns > 0:
                    results['orphan_transactions'] = orphan_txns
                
                # Check orphan items
                query = text(
                    f"SELECT COUNT(*) FROM {schema}.transaction_items i "
                    f"LEFT JOIN {schema}.transactions t ON i.transaction_id = t.transaction_id "
                    f"WHERE t.transaction_id IS NULL"
                )
                orphan_items_tx = conn.execute(query).scalar()
                if orphan_items_tx > 0:
                    results['orphan_items_tx'] = orphan_items_tx
            
            return {
                'status': 'passed' if not results else 'failed',
                'orphan_records': results,
                'total_orphans': sum(results.values())
            }
        except Exception as e:
            logger.error(f"Error checking referential integrity: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def check_data_ranges(self, schema: str = 'staging') -> dict:
        """Check data ranges and constraints"""
        logger.info(f"Checking data ranges in {schema} schema...")
        
        results = {}
        try:
            with self.engine.connect() as conn:
                # Check negative prices
                query = text(
                    f"SELECT COUNT(*) FROM {schema}.products WHERE price <= 0 OR cost <= 0"
                )
                negative_prices = conn.execute(query).scalar()
                if negative_prices > 0:
                    results['negative_prices'] = negative_prices
                
                # Check invalid discounts
                query = text(
                    f"SELECT COUNT(*) FROM {schema}.transaction_items "
                    f"WHERE discount_percentage < 0 OR discount_percentage > 100"
                )
                invalid_discounts = conn.execute(query).scalar()
                if invalid_discounts > 0:
                    results['invalid_discounts'] = invalid_discounts
            
            return {
                'status': 'passed' if not results else 'failed',
                'range_violations': results,
                'total_violations': sum(results.values())
            }
        except Exception as e:
            logger.error(f"Error checking ranges: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def calculate_quality_score(self, check_results: dict) -> float:
        """Calculate overall quality score"""
        logger.info("Calculating quality score...")
        
        total_violations = (
            check_results.get('null_checks', {}).get('total_violations', 0) +
            check_results.get('duplicate_checks', {}).get('total_duplicates', 0) +
            check_results.get('referential_integrity', {}).get('total_orphans', 0) +
            check_results.get('range_checks', {}).get('total_violations', 0)
        )
        
        # Get total records count
        try:
            with self.engine.connect() as conn:
                total_records = 0
                for table in ['customers', 'products', 'transactions', 'transaction_items']:
                    count = conn.execute(
                        text(f"SELECT COUNT(*) FROM staging.{table}")
                    ).scalar()
                    total_records += count
        except:
            total_records = 1
        
        quality_score = max(0, 100 - (total_violations / max(1, total_records) * 100))
        return round(quality_score, 2)

def main():
    logger.info("=" * 50)
    logger.info("Starting Data Quality Checks")
    logger.info("=" * 50)
    
    try:
        validator = DataQualityValidator()
        
        check_results = {
            'null_checks': validator.check_null_values(),
            'duplicate_checks': validator.check_duplicates(),
            'referential_integrity': validator.check_referential_integrity(),
            'range_checks': validator.check_data_ranges()
        }
        
        quality_score = validator.calculate_quality_score(check_results)
        
        report = {
            'check_timestamp': datetime.now().isoformat(),
            'checks_performed': check_results,
            'overall_quality_score': quality_score,
            'quality_grade': 'A' if quality_score >= 95 else 'B' if quality_score >= 85 else 'C' if quality_score >= 75 else 'D'
        }
        
        os.makedirs('data/staging', exist_ok=True)
        with open('data/staging/quality_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("=" * 50)
        logger.info(f"Quality Score: {quality_score}%")
        logger.info(f"Quality Grade: {report['quality_grade']}")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during quality checks: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
