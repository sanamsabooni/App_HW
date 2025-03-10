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

def recreate_tables():
    """Drops tables if they exist and recreates them."""
    conn = get_db_connection()
    if conn is None:
        return

    cur = conn.cursor()
    
    drop_tables = """
    DROP TABLE IF EXISTS zoho_accounts_table CASCADE;
    DROP TABLE IF EXISTS agents CASCADE;
    DROP TABLE IF EXISTS merchants CASCADE;
    """

    create_zoho_accounts_table = """
    CREATE TABLE zoho_accounts_table (
        id SERIAL PRIMARY KEY,
        account_id TEXT UNIQUE,
        partner_name TEXT,
        office_code TEXT ,
        office_code_2 TEXT,
        split TEXT,
        split_2 TEXT,
        pci_fee TEXT,
        merchant_number TEXT UNIQUE,
        sales_id TEXT,
        pci_amnt TEXT,
        account_name TEXT,
        date_approved Date
    );
    """

    create_agents_table = """
    CREATE TABLE agents (
        id SERIAL PRIMARY KEY,
        account_id TEXT UNIQUE,
        partner_name TEXT,
        office_code TEXT,
        office_code_2 TEXT,
        split TEXT,
        split_2 TEXT,
        pci_fee TEXT,
        account_name TEXT UNIQUE
    );
    """

    create_merchants_table = """
    CREATE TABLE merchants (
        id SERIAL PRIMARY KEY,
        account_id TEXT UNIQUE,
        merchant_number TEXT UNIQUE,
        account_name TEXT,
        sales_id TEXT,
        pci_amnt TEXT,
        date_approved Date
    );
    """

    cur.execute(drop_tables)
    cur.execute(create_zoho_accounts_table)
    cur.execute(create_agents_table)
    cur.execute(create_merchants_table)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables dropped and recreated successfully.")

if __name__ == "__main__":
    recreate_tables()