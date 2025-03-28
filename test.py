import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection  # Ensure this function is correctly implemented
import matplotlib.pyplot as plt
import visualization  # Import visualization for HW Visualization

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
page = st.sidebar.radio("Go to", ["Count Tables", "Accounts Full Data", "Orders Full Data", "Products Full Data", "PCI Report", "Equipment Report", "Agents", "Merchants", "HW Visualization"])

# Sub-navigation for HW Visualization
if page == "HW Visualization":
    sub_page = st.sidebar.radio("Select a Visualization", ["Product Locations", "Active Agents", "Test"])
    
    if sub_page == "Product Locations":
        st.header("📍 Product Location Distribution")
        product_locations_data = visualization.show_visualization()
    
    elif sub_page == "Active Agents":
        st.header("📍 Active Agents")
        product_locations_data = visualization.show_visualization()
        
    elif sub_page == "Test":
        st.header("📍 Test")
        product_locations_data = visualization.show_visualization()
    

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
# Count Data for all tables Query
# Load Reports from Database
tables_query = """
    SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table
    UNION ALL 
    SELECT 'Sales Orders' AS table_name, COUNT(*) AS row_count FROM zoho_orders_table
    UNION ALL 
    SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM zoho_products_table
    UNION ALL 
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM agents 
    UNION ALL 
    SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
"""
count_tables = load_data_from_db(tables_query)


# Accounts Full Data Query
accounts_full_data = load_data_from_db("SELECT * FROM zoho_accounts_table;")


count_tables = load_data_from_db("""
    SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table
    UNION ALL 
    SELECT 'Sales Orders' AS table_name, COUNT(*) AS row_count FROM zoho_orders_table
    UNION ALL 
    SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM zoho_products_table
    UNION ALL 
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM agents 
    UNION ALL 
    SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
""")


# Orders Full Data Query
orders_full_data = load_data_from_db("SELECT * FROM zoho_orders_table;")

# Products Full Data Query
products_full_data = load_data_from_db("SELECT * FROM zoho_products_table;")

# Agents & Merchants Queries
agents_data = load_data_from_db("SELECT partner_name, office_code, office_code_2, split, split_2, pci_fee FROM agents;")
merchants_data = load_data_from_db("SELECT merchant_number, account_name, account_status, sales_id, outside_agents,  pci_amnt, date_approved FROM merchants WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';")


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
                               
        ROUND(
            CASE 
                WHEN (COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) < 0 
                THEN (COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0))
                ELSE (COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) * 
                    COALESCE(
                        CASE
                            WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN
                                REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100
                            WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN
                                REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100
                            ELSE 0
                        END, 0)
            END, 
        2) AS pci_share,

                               
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


# Equipment report Query
equipment_report = load_data_from_db("""
    SELECT 
        order_id, 
        so_number, 
        merchant_number, 
        tech_setup_order_options, 
        communication_type, 
        wireless_carrier, 
        terminal_detail, 
        terminal_id, 
        outside_agents, 
        status, 
        est_equip_due_date,
        equipment_received_date,
        tracking_number,
        tracking_number2,                             
        purchase_settled,
        date_shipped,
        location,
        subject,
        product_s_n,
        
        -- Initialize fee-related counters
        0 AS g, 0 AS s, 0 AS c, 0 AS wf, 0 AS w, 0 AS v,

        -- Count occurrences of specific communication types
        (CASE 
            WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma', 'gateway') 
            THEN 1 ELSE 0 
        END) AS "Terminal/Gateway",

        -- Count occurrences of specific terminal details
        (CASE 
            WHEN LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') 
            THEN 1 ELSE 0 
        END) AS "Valor Count"

    FROM zoho_orders_table;
""")


# Load the Equipment Report Pivot Table
equipment_pivot_report = load_data_from_db("""
    SELECT 
        merchant_number,
        SUM(
            CASE 
                WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma', 'gateway') 
                THEN 1 ELSE 0 
            END
        ) AS "Total Terminal/Gateway",
        
        SUM(
            CASE 
                WHEN LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') 
                THEN 1 ELSE 0 
            END
        ) AS "Total Valor Count"
        
    FROM zoho_orders_table
    WHERE LOWER(status) = 'completed'
    GROUP BY merchant_number
    HAVING 
        SUM(
            CASE 
                WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma', 'gateway') 
                THEN 1 ELSE 0 
            END
        ) > 0 
        OR 
        SUM(
            CASE 
                WHEN LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') 
                THEN 1 ELSE 0 
            END
        ) > 0;
""")


# Function to Fetch Product Location Data
def fetch_product_locations():
    query = """
        SELECT location, COUNT(*) as count 
        FROM products 
        WHERE location IS NOT NULL 
        GROUP BY location 
        ORDER BY count DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)
    
    



# Display Selected Page
if page == "Count Tables":
    st.header("📊 Table Counts")
    if count_tables is not None:
        st.dataframe(count_tables)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Accounts Full Data":
    st.header("📂 Accounts Full Data")
    if accounts_full_data is not None:
        st.dataframe(accounts_full_data)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Orders Full Data":
    st.header("📦 Orders Full Data")
    if orders_full_data is not None:
        st.dataframe(orders_full_data)
    else:
        st.warning("No data available. Run the report script first.")  

elif page == "Products Full Data":
    st.header("🔌 Products Full Data")
    if products_full_data is not None:
        st.dataframe(products_full_data)
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

elif page == "PCI Report":
    st.header("🧾 PCI Report")
    if pci_report is not None:
        st.dataframe(pci_report)
    else:
        st.warning("No data available. Run the report script first.")

elif page == "Equipment Report":
    st.header("🖨️ Equipment Report")
    if equipment_report is not None:
        st.dataframe(equipment_report)
    else:
        st.warning("No data available. Run the report script first.")

    # Add a separator
    st.markdown("---")

    # Display Pivot Table Below
    st.header("📊 Equipment Summary (Pivot Table)")
    if equipment_pivot_report is not None:
        st.dataframe(equipment_pivot_report)
    else:
        st.warning("No data available for pivot table.")

# Visualization for HW Visualization
elif page == "HW Visualization":
    if page == "HW Visualization":
        st.header("📍 Product Location Distribution")
        product_locations_data = visualization.show_visualization()


# Refresh Button
if st.button("🔄 Refresh Data"):    
    st.cache_data.clear()
    st.experimental_rerun()

st.sidebar.success("Use the navigation to view different reports.")