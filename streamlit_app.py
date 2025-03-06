import streamlit as st
import pandas as pd
import os
import psycopg2
from utils.db_utils import get_db_connection

# Page Configuration (Must be the first Streamlit command)
st.set_page_config(page_title="HubWallet Reports", layout="wide")

# Load Logo
logo_path = "logo.png"  # Ensure the correct file name

# Use columns to align logo and title closer together
col1, col2 = st.columns([0.2, 3])  # Reduce spacing between logo and text
with col1:
    st.image(logo_path, width=90)  # Ensure logo loads properly
with col2:
    st.markdown("<h1 style='display: inline-block; vertical-align: middle; margin-left: -10px;'>HubWallet Reports Dashboard</h1>", unsafe_allow_html=True)

st.markdown("\n\n")  # Add extra spacing between the header and content

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Count Tables", "Full Data", "PCI Report", "Agents", "Merchants"])

# Database Connection
engine = get_db_connection()

def load_data_from_db(query):
    if engine is not None:
        return pd.read_sql_query(query, engine)
    return None

# Load Reports from database
tables_query = """
    SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table 
    UNION ALL 
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM Agents 
    UNION ALL 
    SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
"""
count_tables = load_data_from_db(tables_query)
full_data = load_data_from_db("SELECT * FROM zoho_accounts_table;")
pci_report = load_data_from_db("""
    SELECT
        CAST(m.merchant_number AS TEXT) AS merchant_number, -- Ensure text format
        m.account_name,
        m.sales_id,
        COALESCE(a.partner_name, 'Unknown') AS agent_name, -- Ensure Agent Name Appears
        TO_CHAR(m.date_approved, 'YYYY-MM') AS approval_month, -- Format approval date
        TO_CHAR(m.date_approved + INTERVAL '2 months', 'Month') AS effective_month, -- Show only month name
        COALESCE(a.pci_fee::NUMERIC, 0) AS pci_fee,
        COALESCE(m.pci_amnt::NUMERIC, 0) AS pci_amnt,
        ROUND(
            COALESCE(
                CASE
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN
                        (REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100)
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN
                        (REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100)
                    ELSE 0
                END, 0
            ), 2
        ) AS split_value,
        COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0) AS pci_difference
    FROM Merchants m
    LEFT JOIN Agents a
    ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code))
    OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
    WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';
""")  # Fetch PCI data from correct table
agents_data = load_data_from_db("SELECT partner_name, office_code, office_code_2, split, split_2, pci_fee FROM agents;")
merchants_data = load_data_from_db("SELECT merchant_number, account_name, sales_id, pci_amnt, date_approved FROM merchants WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';")

# Display Selected Page
if page == "Count Tables":
    st.header("📊 Table Counts")
    if count_tables is not None:
        st.dataframe(count_tables)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Full Data":
    st.header("📂 Full Data")
    if full_data is not None:
        st.dataframe(full_data)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "PCI Report":
    st.header("💰 PCI Report")
    if pci_report is not None:
        st.dataframe(pci_report)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Agents":
    st.header("🧑‍💼 Agents Table")
    if agents_data is not None:
        st.dataframe(agents_data)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Merchants":
    st.header("🏪 Merchants Table")
    if merchants_data is not None:
        st.dataframe(merchants_data)
    else:
        st.warning("No data available. Run the report script first.")

# Refresh Button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

st.sidebar.success("Use the navigation to view different reports.")
