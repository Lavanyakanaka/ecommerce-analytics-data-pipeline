import pytest
import pandas as pd
from scripts.data_generation.generate_data import DataGenerator
import yaml

@pytest.fixture
def config():
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

@pytest.fixture
def generator(config):
    return DataGenerator(config)

def test_generate_customers(generator):
    df = generator.generate_customers(100)
    assert len(df) == 100
    assert 'customer_id' in df.columns
    assert df['email'].duplicated().sum() == 0
    assert df['customer_id'].isnull().sum() == 0

def test_generate_products(generator):
    df = generator.generate_products(50)
    assert len(df) == 50
    assert 'product_id' in df.columns
    assert (df['price'] > df['cost']).all()

def test_generate_transactions(generator):
    customers = generator.generate_customers(100)
    df = generator.generate_transactions(100, customers)
    assert len(df) == 100
    assert df['customer_id'].isin(customers['customer_id']).all()

def test_referential_integrity(generator):
    customers = generator.generate_customers(100)
    products = generator.generate_products(50)
    transactions = generator.generate_transactions(100, customers)
    items = generator.generate_transaction_items(transactions, products)
    
    validation = generator.validate_referential_integrity(customers, products, transactions, items)
    assert validation['orphan_customer_ids'] == 0
    assert validation['quality_score'] > 0
