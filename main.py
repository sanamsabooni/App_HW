from database import recreate_tables
from fetch_data import fetch_and_store_data

if __name__ == "__main__":
    recreate_tables()
    fetch_and_store_data()