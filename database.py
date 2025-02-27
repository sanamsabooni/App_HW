import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ✅ Ensure the correct database connection details
DB_HOST = os.getenv("RDS_HOST")  # Should be: zohocrmdb.cru0k6aaccv5.us-east-1.rds.amazonaws.com
DB_NAME = os.getenv("RDS_DB")  # Should be: ZohoCrmDB
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("✅ Successfully connected to the database!")
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def create_table():
    """Creates the required table for Zoho CRM data if it does not exist."""
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS zoho_accounts (
        account_id VARCHAR(50) PRIMARY KEY,
        pci_fee NUMERIC(10,2),
        pci_amnt NUMERIC(10,2),
        split VARCHAR(50)
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table `zoho_accounts` is set up correctly.")

if __name__ == "__main__":
    create_table()
