import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection  # Ensure this function is correctly implemented

# Page Configuration
st.set_page_config(page_title="HubWallet Reports", layout="wide")

# Load Logo
logo_path = "logo.png"

# Use columns to align logo and title closer together
col1, col2 = st.columns([0.2, 3])
with col1:
    st.image(logo_path, width=90)
with col2:
    st.markdown(
        "<h1 style='display: inline-block; vertical-align: middle; margin-left: -10px;'>HubWallet Reports Dashboard</h1>", 
        unsafe_allow_html=True
    )

st.markdown("\n\n")  # Extra spacing

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Count Tables", "Accounts Full Data", "PCI Report", "Orders Full Data", "Agents", "Merchants"])

# Database Connection
engine = get_db_connection()  # Ensure this function returns a valid SQLAlchemy engine

#**************
#@st.cache_data
#**************
def load_data_from_db(query):
    #"""Fetch data from database and return as Pandas DataFrame."""
    if engine:
        try:
            return pd.read_sql_query(query, engine)
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    #return pd.DataFrame()
    #*********************khate baadi ro hazd kon, balayi ro active kon!
    return None

# Load Reports from Database
tables_query = """
    SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table 
    UNION ALL 
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM agents 
    UNION ALL 
    SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants
    UNION ALL 
    SELECT 'Sales Orders' AS table_name, COUNT(*) AS row_count FROM zoho_orders_table;
"""
count_tables = load_data_from_db(tables_query)
Accounts_full_data = load_data_from_db("SELECT * FROM zoho_accounts_table;")

# PCI Report Query
pci_report = load_data_from_db("""
    SELECT
        CAST(m.merchant_number AS TEXT) AS merchant_number,
        m.account_name,
        m.sales_id,
        COALESCE(a.partner_name, 'Unknown') AS agent_name,
        TO_CHAR(m.date_approved, 'YYYY-MM') AS approval_month,
        TO_CHAR(m.date_approved + INTERVAL '2 months', 'Month') AS effective_month,
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
        ROUND((COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) * 
            COALESCE(
                CASE
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN
                        (REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100)
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN
                        (REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100)
                    ELSE 0
                END, 0), 2) AS pci_share,
        CASE 
            WHEN (COALESCE(m.pci_amnt::NUMERIC, 0) - 
                COALESCE(a.pci_fee::NUMERIC, 0) - 
                ROUND((COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) * 
                        COALESCE(
                            CASE
                                WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN
                                    (REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100)
                                WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN
                                    (REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100)
                                ELSE 0
                            END, 0), 2)) * 0.15 < 0 
            THEN 0
            ELSE ROUND((COALESCE(m.pci_amnt::NUMERIC, 0) - 
                        COALESCE(a.pci_fee::NUMERIC, 0) - 
                        ROUND((COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) * 
                            COALESCE(
                                CASE
                                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN
                                        (REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100)
                                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN
                                        (REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100)
                                    ELSE 0
                                END, 0), 2)) * 0.15, 2)
        END AS max_share

    FROM Merchants m
    LEFT JOIN Agents a
    ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code))
    OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
    WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';
""")


# Remove pci_difference column before displaying
if pci_report is not None:
    pci_report = pci_report.drop(columns=['pci_difference'], errors='ignore')

# Orders Full Data Query
Orders_Full_Data_query = """
    SELECT 
        order_id, so_number, merchant_number, tech_setup_order_options, communication_type, 
        wireless_carrier, terminal_detail, terminal_id, outside_agent, outside_agents, 
        status, equipment_received_date 
    FROM zoho_orders_table;
"""
Orders_Full_Data = load_data_from_db(Orders_Full_Data_query)

# Sales Orders Query
sales_orders_query = """
    SELECT order_id, so_number, merchant_number, account_name, status, equipment_received_date
    FROM zoho_orders_table
    ORDER BY equipment_received_date DESC;
"""
sales_orders_data = load_data_from_db(sales_orders_query)

# Agents & Merchants Queries
agents_data = load_data_from_db("SELECT partner_name, office_code, office_code_2, split, split_2, pci_fee FROM agents;")
merchants_data = load_data_from_db("SELECT merchant_number, account_name, sales_id, pci_amnt, date_approved FROM merchants WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';")

# Display Selected Page
# Display Selected Page
if page == "Count Tables":
    st.header("📊 Table Counts")
    if count_tables is not None:
        st.dataframe(count_tables)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Accounts Full Data":
    st.header("📂 Accounts Full Data")
    if Accounts_full_data is not None:
        st.dataframe(Accounts_full_data)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "PCI Report":
    st.header("💰 PCI Report")
    if pci_report is not None:
        st.dataframe(pci_report)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Orders Full Data":
    st.header("🛠 Orders Full Data")
    if Orders_Full_Data is not None:
        st.dataframe(Orders_Full_Data)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Sales Orders":
    st.header("📑 Sales Orders Report")
    if sales_orders_data is not None:
        st.dataframe(sales_orders_data)
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
