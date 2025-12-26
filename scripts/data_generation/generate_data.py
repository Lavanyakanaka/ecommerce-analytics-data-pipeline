"""
Data Generation Script for E-Commerce Pipeline
Generates synthetic customer, product, transaction, and transaction_items data
"""

import pandas as pd
import numpy as np
from faker import Faker
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
def load_config():
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

config = load_config()

class DataGenerator:
    def __init__(self, config):
        self.config = config['data_generation']
        self.faker = Faker()
        np.random.seed(42)
        Faker.seed(42)
        
    def generate_customers(self, num_customers: int) -> pd.DataFrame:
        """Generate customer data"""
        logger.info(f"Generating {num_customers} customers...")
        
        data = {
            'customer_id': [f"CUST{i+1:04d}" for i in range(num_customers)],
            'first_name': [self.faker.first_name() for _ in range(num_customers)],
            'last_name': [self.faker.last_name() for _ in range(num_customers)],
            'email': [self.faker.unique.email() for _ in range(num_customers)],
            'phone': [self.faker.phone_number() for _ in range(num_customers)],
            'registration_date': [
                self.faker.date_between(start_date='-2y').isoformat() 
                for _ in range(num_customers)
            ],
            'city': [self.faker.city() for _ in range(num_customers)],
            'state': [self.faker.state() for _ in range(num_customers)],
            'country': ['USA'] * num_customers,
            'age_group': np.random.choice(
                ['18-25', '26-35', '36-45', '46-55', '56-65', '65+'], 
                num_customers
            )
        }
        
        df = pd.DataFrame(data)
        logger.info(f"✓ Generated {len(df)} customers")
        return df
    
    def generate_products(self, num_products: int) -> pd.DataFrame:
        """Generate product data"""
        logger.info(f"Generating {num_products} products...")
        
        categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports', 'Beauty']
        subcategories = {
            'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones'],
            'Clothing': ['Men', 'Women', 'Kids', 'Shoes'],
            'Home & Kitchen': ['Furniture', 'Cookware', 'Bedding'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational'],
            'Sports': ['Equipment', 'Apparel', 'Accessories'],
            'Beauty': ['Skincare', 'Makeup', 'Haircare']
        }
        
        products_data = []
        for i in range(num_products):
            category = np.random.choice(categories)
            subcategory = np.random.choice(subcategories[category])
            cost = np.random.uniform(10, 500)
            price = cost * np.random.uniform(1.2, 3.0)  # Ensure profit margin
            
            products_data.append({
                'product_id': f"PROD{i+1:04d}",
                'product_name': self.faker.word() + ' ' + self.faker.word(),
                'category': category,
                'sub_category': subcategory,
                'price': round(price, 2),
                'cost': round(cost, 2),
                'brand': self.faker.company()[:20],
                'stock_quantity': np.random.randint(0, 1000),
                'supplier_id': f"SUP{np.random.randint(1, 51):03d}"
            })
        
        df = pd.DataFrame(products_data)
        logger.info(f"✓ Generated {len(df)} products")
        return df
    
    def generate_transactions(self, num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
        """Generate transaction data"""
        logger.info(f"Generating {num_transactions} transactions...")
        
        payment_methods = ['Credit Card', 'Debit Card', 'UPI', 'Net Banking', 'Cash on Delivery']
        start_date = datetime.strptime(self.config['transaction_date_start'], '%Y-%m-%d')
        end_date = datetime.strptime(self.config['transaction_date_end'], '%Y-%m-%d')
        
        transactions_data = []
        for i in range(num_transactions):
            tx_date = start_date + timedelta(days=np.random.randint(0, (end_date - start_date).days))
            
            transactions_data.append({
                'transaction_id': f"TXN{i+1:05d}",
                'customer_id': np.random.choice(customers_df['customer_id'].values),
                'transaction_date': tx_date.date().isoformat(),
                'transaction_time': f"{np.random.randint(0,24):02d}:{np.random.randint(0,60):02d}:{np.random.randint(0,60):02d}",
                'payment_method': np.random.choice(payment_methods),
                'shipping_address': self.faker.address().replace('\n', ', '),
                'total_amount': 0.0  # Will be calculated after items are added
            })
        
        df = pd.DataFrame(transactions_data)
        logger.info(f"✓ Generated {len(df)} transactions")
        return df
    
    def generate_transaction_items(self, transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Generate transaction items data"""
        logger.info("Generating transaction items...")
        
        items_data = []
        item_id_counter = 1
        total_items_target = np.random.randint(
            self.config['num_items_min'], 
            self.config['num_items_max']
        )
        
        items_per_tx = max(1, total_items_target // len(transactions_df))
        
        for tx_idx, tx_row in transactions_df.iterrows():
            num_items = np.random.randint(1, items_per_tx + 2)
            
            for _ in range(num_items):
                product = products_df.sample(1).iloc[0]
                quantity = np.random.randint(1, 5)
                unit_price = float(product['price'])
                discount_pct = np.random.choice([0, 0, 0, 5, 10, 15, 20], p=[0.4, 0.2, 0.2, 0.1, 0.05, 0.03, 0.02])
                line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)
                
                items_data.append({
                    'item_id': f"ITEM{item_id_counter:05d}",
                    'transaction_id': tx_row['transaction_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': round(unit_price, 2),
                    'discount_percentage': discount_pct,
                    'line_total': line_total
                })
                
                item_id_counter += 1
                
                # Update transaction total
                transactions_df.loc[tx_idx, 'total_amount'] = round(
                    transactions_df.loc[tx_idx, 'total_amount'] + line_total, 2
                )
        
        df = pd.DataFrame(items_data)
        logger.info(f"✓ Generated {len(df)} transaction items")
        return df
    
    def validate_referential_integrity(self, customers: pd.DataFrame, products: pd.DataFrame,
                                       transactions: pd.DataFrame, items: pd.DataFrame) -> dict:
        """Validate referential integrity"""
        logger.info("Validating referential integrity...")
        
        issues = {
            'orphan_customer_ids': 0,
            'orphan_product_ids': 0,
            'orphan_transaction_ids': 0,
            'constraint_violations': 0,
            'quality_score': 100.0
        }
        
        # Check orphan transactions
        orphan_txns = transactions[~transactions['customer_id'].isin(customers['customer_id'])]
        issues['orphan_customer_ids'] = len(orphan_txns)
        
        # Check orphan items
        orphan_items_tx = items[~items['transaction_id'].isin(transactions['transaction_id'])]
        orphan_items_prod = items[~items['product_id'].isin(products['product_id'])]
        issues['orphan_transaction_ids'] = len(orphan_items_tx) + len(orphan_items_prod)
        
        # Check constraint violations
        violations = len(products[products['price'] <= products['cost']])
        issues['constraint_violations'] = violations
        
        # Calculate quality score
        total_records = len(customers) + len(products) + len(transactions) + len(items)
        total_issues = sum([issues['orphan_customer_ids'], issues['orphan_product_ids'],
                           issues['orphan_transaction_ids'], issues['constraint_violations']])
        
        issues['quality_score'] = max(0, 100 - (total_issues / total_records * 100))
        
        logger.info(f"✓ Referential integrity check complete")
        logger.info(f"  Orphan records: {total_issues}")
        logger.info(f"  Quality score: {issues['quality_score']:.2f}%")
        
        return issues
    
    def save_to_csv(self, customers: pd.DataFrame, products: pd.DataFrame,
                    transactions: pd.DataFrame, items: pd.DataFrame):
        """Save data to CSV files"""
        logger.info("Saving data to CSV files...")
        
        os.makedirs('data/raw', exist_ok=True)
        
        customers.to_csv('data/raw/customers.csv', index=False)
        products.to_csv('data/raw/products.csv', index=False)
        transactions.to_csv('data/raw/transactions.csv', index=False)
        items.to_csv('data/raw/transaction_items.csv', index=False)
        
        logger.info("✓ Data saved to CSV")
    
    def save_metadata(self, customers: pd.DataFrame, products: pd.DataFrame,
                     transactions: pd.DataFrame, items: pd.DataFrame):
        """Save generation metadata"""
        logger.info("Saving metadata...")
        
        metadata = {
            'generation_timestamp': datetime.now().isoformat(),
            'record_counts': {
                'customers': len(customers),
                'products': len(products),
                'transactions': len(transactions),
                'transaction_items': len(items)
            },
            'date_range': {
                'start': self.config['transaction_date_start'],
                'end': self.config['transaction_date_end']
            },
            'total_revenue': round(float(transactions['total_amount'].sum()), 2)
        }
        
        os.makedirs('data/raw', exist_ok=True)
        with open('data/raw/generation_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info("✓ Metadata saved")
        return metadata

def main():
    logger.info("=" * 50)
    logger.info("Starting Data Generation")
    logger.info("=" * 50)
    
    try:
        generator = DataGenerator(config)
        
        # Generate all data
        customers = generator.generate_customers(config['data_generation']['num_customers'])
        products = generator.generate_products(config['data_generation']['num_products'])
        transactions = generator.generate_transactions(config['data_generation']['num_transactions'], customers)
        items = generator.generate_transaction_items(transactions, products)
        
        # Validate
        validation = generator.validate_referential_integrity(customers, products, transactions, items)
        
        # Save
        generator.save_to_csv(customers, products, transactions, items)
        metadata = generator.save_metadata(customers, products, transactions, items)
        
        logger.info("=" * 50)
        logger.info("✓ Data Generation Complete!")
        logger.info("=" * 50)
        logger.info(f"Generated Records:")
        logger.info(f"  - Customers: {len(customers)}")
        logger.info(f"  - Products: {len(products)}")
        logger.info(f"  - Transactions: {len(transactions)}")
        logger.info(f"  - Items: {len(items)}")
        logger.info(f"Quality Score: {validation['quality_score']:.2f}%")
        logger.info(f"Total Revenue: ${metadata['total_revenue']:,.2f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during data generation: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
