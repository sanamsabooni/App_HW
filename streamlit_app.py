import streamlit as st
import pandas as pd
from database import get_db_connection

def fetch_paginated_data(page=1, per_page=200):
    """Fetch paginated data from the database."""
    conn = get_db_connection()
    if not conn:
        st.error("Database connection failed!")
        return None

    cur = conn.cursor()
    offset = (page - 1) * per_page
    query = """
        SELECT partner_name, office_code, office_code_2, split, pci_fee, sales_id, pci_amnt, account_name, outside_agent
        FROM accounts
        ORDER BY sales_id
        LIMIT %s OFFSET %s
    """
    cur.execute(query, (per_page, offset))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    columns = ["Partner Name", "Office Code", "Office Code 2", "Split", "PCI Fee", "Sales ID", "PCI Amount", "Account Name", "Outside Agent"]
    return pd.DataFrame(rows, columns=columns) if rows else None

# Streamlit UI for pagination
st.title("Zoho CRM Data Viewer")

# Pagination Controls
page = st.number_input("Page Number", min_value=1, step=1, value=1)
per_page = 200

# Fetch and display data
data = fetch_paginated_data(page=page, per_page=per_page)
if data is not None and not data.empty:
    st.dataframe(data)
else:
    st.warning("No data found.")
