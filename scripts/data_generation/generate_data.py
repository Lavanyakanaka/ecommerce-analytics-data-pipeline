import pandas as pd
import json
from faker import Faker
from datetime import datetime
from pathlib import Path
import random

fake = Faker()
RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

def generate_customers(num_customers: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "customer_id": i + 1,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "gender": random.choice(["M", "F"]),
        "signup_date": fake.date_this_decade()
    } for i in range(num_customers)])

def generate_products(num_products: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "product_id": i + 1,
        "product_name": fake.word(),
        "category": random.choice(["Electronics", "Clothing", "Home"]),
        "price": round(random.uniform(10, 500), 2)
    } for i in range(num_products)])

def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "transaction_id": i + 1,
        "customer_id": random.choice(customers_df["customer_id"]),
        "transaction_date": fake.date_time_this_year(),
        "payment_method": random.choice(["Card", "UPI", "Cash"]),
        "total_amount": round(random.uniform(50, 1000), 2)
    } for i in range(num_transactions)])

def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    tid = 1
    for _, tx in transactions_df.iterrows():
        for _ in range(random.randint(1, 3)):
            product = products_df.sample(1).iloc[0]
            rows.append({
                "transaction_item_id": tid,
                "transaction_id": tx["transaction_id"],
                "product_id": product["product_id"],
                "quantity": random.randint(1, 5),
                "unit_price": product["price"]
            })
            tid += 1
    return pd.DataFrame(rows)

def validate_referential_integrity(customers, products, transactions, items) -> dict:
    return {
        "customer_fk_valid": bool(transactions["customer_id"].isin(customers["customer_id"]).all()),
        "product_fk_valid": bool(items["product_id"].isin(products["product_id"]).all()),
        "transaction_fk_valid": bool(items["transaction_id"].isin(transactions["transaction_id"]).all())
    }


def main():
    customers = generate_customers(100)
    products = generate_products(50)
    transactions = generate_transactions(200, customers)
    items = generate_transaction_items(transactions, products)

    customers.to_csv(RAW_PATH / "customers.csv", index=False)
    products.to_csv(RAW_PATH / "products.csv", index=False)
    transactions.to_csv(RAW_PATH / "transactions.csv", index=False)
    items.to_csv(RAW_PATH / "transaction_items.csv", index=False)

    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "validation": validate_referential_integrity(customers, products, transactions, items)
    }

    with open(RAW_PATH / "generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

if __name__ == "__main__":
    main()
