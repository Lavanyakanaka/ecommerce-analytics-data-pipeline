from extract.generate_data import generate_data
from transform.transform_data import transform_data
from load.load_to_postgres import load_data

if __name__ == "__main__":
    generate_data()
    transform_data()
    load_data()
