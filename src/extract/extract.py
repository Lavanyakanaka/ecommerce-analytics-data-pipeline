import pandas as pd
from pathlib import Path

RAW = Path("data/raw")

def extract():
    customers = pd.read_csv(RAW / "customers.csv")
    products = pd.read_csv(RAW / "products.csv")
    return customers, products
