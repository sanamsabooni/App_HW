import streamlit as st
import pandas as pd
from database import get_db_connection

def fetch_merchant_data():
    """Fetch merchant-agent data with correct column order."""
    conn = get_db_connection()
    if not conn:
        st.error("❌ Database connection failed!")
        return None

    cur = conn.cursor()
    query = """
        SELECT 
            merchants.sales_id,
            merchants.pci_amnt,
            merchants.account_name,
            merchants.outside_agent,
            merchants.split,
            merchants.pci_fee,
            COALESCE(agents.partner_name, 'No Agent') AS partner_name,
            COALESCE(agents.office_code, 'N/A') AS office_code,
            COALESCE(agents.office_code_2, 'N/A') AS office_code_2
        FROM merchants
        LEFT JOIN agents ON merchants.outside_agent = agents.partner_name
        ORDER BY merchants.sales_id;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return None

    # Set the correct column order
    columns = [
        "Sales ID", "PCI Amount", "Account Name", "Outside Agent", 
        "Split", "Split2", "PCI Fee", "Partner Name", "Office Code", "Office Code 2"
    ]
    return pd.DataFrame(rows, columns=columns)

# Streamlit UI
st.title("📊 Merchant & Agent Viewer")

# Fetch and display data
data = fetch_merchant_data()
if data is not None and not data.empty:
    st.dataframe(data)
else:
    st.warning("⚠️ No data found. Try running `python fetch_data.py` again.")
