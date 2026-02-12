import pandas as pd
from sqlalchemy import create_engine
import yaml

def load_data():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    db = config["database"]
    engine = create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
    )

    df = pd.read_csv("data/raw/orders.csv")

    df.to_sql("orders", engine, if_exists="append", index=False)
    print("✅ Data loaded into PostgreSQL")

if __name__ == "__main__":
    load_data()
