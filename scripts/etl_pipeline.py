from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

def main():
    customers, products = extract()
    customers, products = transform(customers, products)
    load(customers, products)
    print("ETL Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
