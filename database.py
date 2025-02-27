import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def create_accounts_table():
    """Creates the accounts table if it does not exist."""
    conn = get_db_connection()
    if conn is None:
        return

    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS accounts (
        id SERIAL PRIMARY KEY,
        partner_name TEXT,
        office_code TEXT,
        office_code_2 TEXT,
        split TEXT,
        pci_fee TEXT,
        sales_id TEXT UNIQUE,
        pci_amnt TEXT,
        account_name TEXT,
        outside_agent TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("Accounts table checked/created successfully.")

if __name__ == "__main__":
    create_accounts_table()
