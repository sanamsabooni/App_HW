import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def insert_zoho_record(record):
    """Inserts a single record into the database."""
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO zoho_accounts (account_id, pci_fee, pci_amnt, split) 
        VALUES (%s, %s, %s, %s) 
        ON CONFLICT (account_id) DO UPDATE SET pci_fee = EXCLUDED.pci_fee, pci_amnt = EXCLUDED.pci_amnt, split = EXCLUDED.split
    """, (record.get("id"), record.get("PCI_Fee"), record.get("PCI_Amount"), record.get("Split")))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Record inserted/updated successfully.")
