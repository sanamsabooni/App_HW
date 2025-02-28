import streamlit as st
import pandas as pd
from database import get_db_connection

def fetch_merchant_data():
    """Fetch joined merchant-agent data."""
    conn = get_db_connection()
    if not conn:
        st.error("❌ Database connection failed!")
        return None

    cur = conn.cursor()
    query = """
        SELECT 
            merchants.account_name,
            merchants.sales_id,
            merchants.pci_fee,
            merchants.pci_amnt,
            merchants.split,
            COALESCE(agents.partner_name, 'No Agent') AS agent_name,
            COALESCE(agents.office_code, 'N/A') AS office_code,
            COALESCE(agents.office_code_2, 'N/A') AS office_code_2
        FROM merchants
        LEFT JOIN agents ON merchants.outside_agent = agents.partner_name
        ORDER BY merchants.account_name;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return None

    columns = ["Merchant Name", "Sales ID", "PCI Fee", "PCI Amount", "Split", "Agent Name", "Agent Office Code", "Agent Office Code 2"]
    return pd.DataFrame(rows, columns=columns)

# Streamlit UI
st.title("📊 Merchant & Agent Viewer")

# Fetch and display data
data = fetch_merchant_data()
if data is not None and not data.empty:
    st.dataframe(data)
else:
    st.warning("⚠️ No data found. Try running `python fetch_data.py` again.")
