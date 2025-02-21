import pandas as pd
from database import get_db_connection
import psycopg2
from database import get_db_connection

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


def get_all_contacts():
    """Retrieve all contacts from PostgreSQL."""
    conn, tunnel = get_db_connection()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame if connection fails

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT zoho_id, full_name, email FROM zoho_contacts;")
        rows = cursor.fetchall()
        conn.close()
        tunnel.stop()

        # Convert the fetched data to a Pandas DataFrame
        df = pd.DataFrame(rows, columns=["Zoho ID", "Agent Name", "Email"])
        df.insert(0, "S.No", range(1, len(df) + 1))  # Add serial numbers
        return df
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error
