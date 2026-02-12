from scripts.db_connection import get_engine

def load(customers, products):
    engine = get_engine()
    customers.to_sql("customers", engine, if_exists="replace", index=False)
    products.to_sql("products", engine, if_exists="replace", index=False)
