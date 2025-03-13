import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database credentials from environment variables
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establish and return a database connection."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def recreate_tables():
    """Drops tables if they exist and recreates them."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                drop_tables_sql = """
                DROP TABLE IF EXISTS zoho_accounts_table CASCADE;
                DROP TABLE IF EXISTS agents CASCADE;
                DROP TABLE IF EXISTS merchants CASCADE;
                DROP TABLE IF EXISTS zoho_orders_table CASCADE;
                """

                create_zoho_accounts_table_sql = """
                CREATE TABLE zoho_accounts_table (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    partner_name TEXT,
                    office_code TEXT,
                    office_code_2 TEXT,
                    split TEXT,
                    split_2 TEXT,
                    pci_fee TEXT,
                    merchant_number TEXT UNIQUE,
                    sales_id TEXT,
                    pci_amnt TEXT,
                    account_name TEXT,
                    date_approved DATE,
                    account_status TEXT
                );
                """

                create_agents_table_sql = """
                CREATE TABLE agents (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    partner_name TEXT,
                    office_code TEXT,
                    office_code_2 TEXT,
                    split TEXT,
                    split_2 TEXT,
                    pci_fee TEXT,
                    account_name TEXT UNIQUE
                );
                """

                create_merchants_table_sql = """
                CREATE TABLE merchants (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    merchant_number TEXT UNIQUE NOT NULL,
                    account_name TEXT,
                    sales_id TEXT,
                    pci_amnt TEXT,
                    date_approved DATE,
                    account_status TEXT
                );
                """

                create_zoho_orders_table_sql = """
                CREATE TABLE zoho_orders_table (
                    order_id  SERIAL PRIMARY KEY,
                    so_number TEXT UNIQUE,
                    merchant_number TEXT,
                    account_name TEXT,
                    tech_setup_order_options TEXT,
                    communication_type TEXT, 
                    wireless_carrier TEXT,
                    terminal_detail TEXT,
                    terminal_id TEXT,
                    outside_agent TEXT,
                    outside_agents TEXT,
                    status TEXT, 
                    equipment_received_date DATE
                );
                """

                # Execute SQL statements
                cur.execute(drop_tables_sql)
                cur.execute(create_zoho_accounts_table_sql)
                cur.execute(create_agents_table_sql)
                cur.execute(create_merchants_table_sql)
                cur.execute(create_zoho_orders_table_sql)

                print("✅ Tables dropped and recreated successfully.")

    except psycopg2.Error as e:
        print(f"❌ Error while recreating tables: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    recreate_tables()
