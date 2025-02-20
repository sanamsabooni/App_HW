from database import get_db_connection

def save_contacts_to_db(contacts):
    """Save contacts to PostgreSQL database."""
    conn, tunnel = get_db_connection()
    if not conn:
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
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
