import pandas as pd
from database import get_db_connection

def save_contacts_to_db(contacts):
    """Save Zoho CRM contacts to PostgreSQL."""
    conn, tunnel = get_db_connection()
    if not conn:
        print("❌ Database connection failed.")
        return

    try:
        cursor = conn.cursor()

        for contact in contacts:
            zoho_id = contact.get("id", "")
            full_name = contact.get("Full_Name", "N/A")
            email = contact.get("Email", "N/A")

            cursor.execute("""
                INSERT INTO zoho_contacts (zoho_id, full_name, email)
                VALUES (%s, %s, %s)
                ON CONFLICT (zoho_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                email = EXCLUDED.email;
            """, (zoho_id, full_name, email))

        conn.commit()
        cursor.close()
        conn.close()
        tunnel.stop()
        print("✅ Contacts saved to PostgreSQL successfully!")
    except Exception as e:
        print(f"❌ Database error: {str(e)}")

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
