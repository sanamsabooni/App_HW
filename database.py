import os
import psycopg2
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# Load environment variables
load_dotenv()

SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

DB_HOST = os.getenv("RDS_HOST")
DB_PORT = int(os.getenv("RDS_PORT", 5432))
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes an SSH tunnel and connects to the PostgreSQL database."""
    try:
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        )
        tunnel.start()

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn, tunnel

    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None, None

def create_table():
    """Creates the required table if it does not exist."""
    conn, tunnel = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS zoho_accounts (
        account_id VARCHAR(50) PRIMARY KEY,
        layout VARCHAR(20),
        pci_fee NUMERIC(10,2),
        pci_amnt NUMERIC(10,2),
        split_percentage NUMERIC(5,2)
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    tunnel.stop()
    print("✅ Table created successfully.")

def save_accounts_to_db(accounts_data):
    """Inserts or updates accounts data into PostgreSQL."""
    conn, tunnel = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()
    insert_query = """
    INSERT INTO zoho_accounts (account_id, layout, pci_fee, pci_amnt, split_percentage)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (account_id) DO UPDATE
    SET pci_fee = EXCLUDED.pci_fee,
        pci_amnt = EXCLUDED.pci_amnt,
        split_percentage = EXCLUDED.split_percentage;
    """

    cur.executemany(insert_query, accounts_data)
    conn.commit()
    cur.close()
    conn.close()
    tunnel.stop()
    print("✅ Data successfully inserted into PostgreSQL.")

if __name__ == "__main__":
    create_table()
