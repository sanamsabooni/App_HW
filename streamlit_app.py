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
    SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM products_at_merchants_table
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
    SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM products_at_merchants_table
    UNION ALL
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM agents 
    UNION ALL 
    SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
""")


# Orders Full Data Query
orders_full_data = load_data_from_db("SELECT * FROM zoho_orders_table;")

# Products Full Data Query
products_full_data = load_data_from_db("SELECT * FROM zoho_products_table;")

# Products at Merchant Location Data Query
products_Merchant_Location_data = load_data_from_db("SELECT * FROM products_at_merchants_table;")

# Agents & Merchants Queries
agents_data = load_data_from_db("SELECT partner_name, office_code, office_code_2, split, split_2, pci_fee FROM agents;")
merchants_data = load_data_from_db("SELECT merchant_number, account_name, account_status, sales_id, outside_agents,  pci_amnt, date_approved, mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal, mpa_valor_virtual_terminal, mpa_valor_ecommerce FROM merchants WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';")


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

    FROM merchants m
    LEFT JOIN agents a
        ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code))
        OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
    WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
    AND m.account_status = 'Approved';
""")
# Remove pci_difference column before displaying
if pci_report is not None:
    pci_report = pci_report.drop(columns=['pci_difference'], errors='ignore')


# Equipment report Query
equipment_report = load_data_from_db("""
    SELECT 
        o.order_id, 
        o.so_number, 
        o.merchant_number, 
        o.tech_setup_order_options, 
        o.communication_type, 
        o.wireless_carrier, 
        o.terminal_detail, 
        o.terminal_id, 
        o.outside_agents, 
        o.status, 
        o.est_equip_due_date,
        o.equipment_received_date,
        o.tracking_number,
        o.tracking_number2,                             
        o.purchase_settled,
        o.date_shipped,
        o.location,
        o.subject,
        o.product_s_n,

        -- Count occurrences of specific communication types
        (CASE 
            WHEN LOWER(o.communication_type) IN ('wireless - gprs', 'wireless - cdma') 
            THEN 1 ELSE 0 
        END) AS "Terminal Count",
                                     
        -- Count occurrences of specific communication types
        (CASE 
            WHEN LOWER(o.communication_type) IN ('gateway') 
            THEN 1 ELSE 0 
        END) AS "Gateway Count",

        -- Count occurrences of valor terminal details
        (CASE 
            WHEN LOWER(o.terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') 
            THEN 1 ELSE 0 
        END) AS "Valor Count",

        -- Fields from merchants table
        m.mpa_wireless_fee,
        m.mpa_valor_portal_access,
        m.mpa_valor_add_on_terminal,
        m.mpa_valor_virtual_terminal,
        m.mpa_valor_ecommerce

    FROM zoho_orders_table o
    LEFT JOIN merchants m
        ON o.merchant_number = m.merchant_number;
""")



# The Equipment Report for Merchants
equipment_pivot_report = load_data_from_db("""
    SELECT 
        sub.merchant_number,
        m.outside_agents,
        sub."Terminal Count",
        sub."Gateway Count",
        sub."Valor Count",

        CAST(sub."Terminal Count" * 10 AS DECIMAL(10,2)) AS "Terminal Fee",
        CAST(sub."Gateway Count" * 10 AS DECIMAL(10,2)) AS "Gateway Fee",

        CASE
            WHEN sub."Valor Count" = 1 THEN 5
            WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
            ELSE 0
        END AS "Valor Fee",

        CAST(
            (sub."Terminal Count" * 10) + 
            (sub."Gateway Count" * 10) +
            CASE
                WHEN sub."Valor Count" = 1 THEN 5
                WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
                ELSE 0
            END
        AS DECIMAL(10,2)) AS "Equipments Fee",

        -- Cast MPA fields early
        CAST(m.mpa_wireless_fee AS NUMERIC) AS mpa_wireless_fee,
        CAST(m.mpa_valor_portal_access AS NUMERIC) AS mpa_valor_portal_access,
        CAST(m.mpa_valor_add_on_terminal AS NUMERIC) AS mpa_valor_add_on_terminal,
        CAST(m.mpa_valor_virtual_terminal AS NUMERIC) AS mpa_valor_virtual_terminal,
        CAST(m.mpa_valor_ecommerce AS NUMERIC) AS mpa_valor_ecommerce,

        -- ✅ Merchant Fees
        CASE WHEN sub."Terminal Count" > 0 THEN CAST(m.mpa_wireless_fee AS NUMERIC) END AS "Merchant Wireless Terminal Fee",
        CASE WHEN sub."Valor Count" > 0 THEN CAST(m.mpa_valor_portal_access AS NUMERIC) END AS "Merchant Valor Fee",
        CASE WHEN sub."Valor Count" > 1 THEN CAST(m.mpa_valor_add_on_terminal AS NUMERIC) END AS "Merchant second Valor Fee",
        CASE WHEN sub."Gateway Count" > 0 THEN COALESCE(CAST(m.mpa_valor_virtual_terminal AS NUMERIC), CAST(m.mpa_valor_ecommerce AS NUMERIC)) END AS "Merchant Gateway Fee",

        -- ✅ Total Merchant Share
        CAST(
            COALESCE(CASE WHEN sub."Terminal Count" > 0 THEN CAST(m.mpa_wireless_fee AS NUMERIC) END, 0) +
            COALESCE(CASE WHEN sub."Valor Count" > 0 THEN CAST(m.mpa_valor_portal_access AS NUMERIC) END, 0) +
            COALESCE(CASE WHEN sub."Valor Count" > 1 THEN CAST(m.mpa_valor_add_on_terminal AS NUMERIC) END, 0) +
            COALESCE(CASE WHEN sub."Gateway Count" > 0 THEN COALESCE(CAST(m.mpa_valor_virtual_terminal AS NUMERIC), CAST(m.mpa_valor_ecommerce AS NUMERIC)) END, 0)
        AS DECIMAL(10,2)) AS "Total Merchant Share",

        -- ✅ Agent Share
        CAST(
            CASE 
                WHEN (
                    (sub."Terminal Count" * 10) + 
                    (sub."Gateway Count" * 10) +
                    CASE
                        WHEN sub."Valor Count" = 1 THEN 5
                        WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
                        ELSE 0
                    END
                    -
                    (
                        COALESCE(CASE WHEN sub."Terminal Count" > 0 THEN CAST(m.mpa_wireless_fee AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 0 THEN CAST(m.mpa_valor_portal_access AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 1 THEN CAST(m.mpa_valor_add_on_terminal AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Gateway Count" > 0 THEN COALESCE(CAST(m.mpa_valor_virtual_terminal AS NUMERIC), CAST(m.mpa_valor_ecommerce AS NUMERIC)) END, 0)
                    )
                ) < 0
                THEN (
                    (
                        (sub."Terminal Count" * 10) + 
                        (sub."Gateway Count" * 10) +
                        CASE
                            WHEN sub."Valor Count" = 1 THEN 5
                            WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
                            ELSE 0
                        END
                    )
                    -
                    (
                        COALESCE(CASE WHEN sub."Terminal Count" > 0 THEN CAST(m.mpa_wireless_fee AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 0 THEN CAST(m.mpa_valor_portal_access AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 1 THEN CAST(m.mpa_valor_add_on_terminal AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Gateway Count" > 0 THEN COALESCE(CAST(m.mpa_valor_virtual_terminal AS NUMERIC), CAST(m.mpa_valor_ecommerce AS NUMERIC)) END, 0)
                    )
                ) * COALESCE(
                    CASE 
                        WHEN m.sales_id = a.office_code THEN CAST(REGEXP_REPLACE(a.split, '[^0-9]', '', 'g') AS NUMERIC) / 100
                        WHEN m.sales_id = a.office_code_2 THEN CAST(REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g') AS NUMERIC) / 100
                        ELSE 1
                    END, 1)
                ELSE (
                    (sub."Terminal Count" * 10) + 
                    (sub."Gateway Count" * 10) +
                    CASE
                        WHEN sub."Valor Count" = 1 THEN 5
                        WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
                        ELSE 0
                    END
                    -
                    (
                        COALESCE(CASE WHEN sub."Terminal Count" > 0 THEN CAST(m.mpa_wireless_fee AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 0 THEN CAST(m.mpa_valor_portal_access AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Valor Count" > 1 THEN CAST(m.mpa_valor_add_on_terminal AS NUMERIC) END, 0) +
                        COALESCE(CASE WHEN sub."Gateway Count" > 0 THEN COALESCE(CAST(m.mpa_valor_virtual_terminal AS NUMERIC), CAST(m.mpa_valor_ecommerce AS NUMERIC)) END, 0)
                    )
                )
            END
        AS DECIMAL(10,2)) AS "Agent Share"

    FROM (
        SELECT 
            merchant_number,
            SUM(CASE WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma') THEN 1 ELSE 0 END) AS "Terminal Count",
            SUM(CASE WHEN LOWER(communication_type) = 'gateway' THEN 1 ELSE 0 END) AS "Gateway Count",
            SUM(CASE WHEN LOWER(terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') THEN 1 ELSE 0 END) AS "Valor Count"
        FROM zoho_orders_table
        WHERE merchant_number IS NOT NULL AND TRIM(merchant_number) <> ''
        GROUP BY merchant_number
    ) AS sub
    LEFT JOIN merchants m ON sub.merchant_number = m.merchant_number
    LEFT JOIN agents a ON m.sales_id IN (a.office_code, a.office_code_2)
    ORDER BY "Valor Count" DESC;
""")




# The Equipment Report for Agents
equipment_agent_charges = load_data_from_db("""
    WITH merchant_level_report AS (
        SELECT 
            m.outside_agents,
            
            CAST(
                (
                    (sub."Terminal Count" * 10) + 
                    (sub."Gateway Count" * 10) +
                    CASE
                        WHEN sub."Valor Count" = 1 THEN 5
                        WHEN sub."Valor Count" > 1 THEN 5 + ((sub."Valor Count" - 1) * 2)
                        ELSE 0
                    END
                ) -
                (
                    COALESCE(
                        CASE 
                            WHEN CAST(sub."Terminal Count" AS DECIMAL(10,2)) > 0 
                            THEN CAST(m.mpa_wireless_fee AS DECIMAL(10,2))
                        END, 0
                    ) +
                    COALESCE(
                        CASE 
                            WHEN CAST(sub."Valor Count" AS DECIMAL(10,2)) > 0 
                            THEN CAST(m.mpa_valor_portal_access AS DECIMAL(10,2))
                        END, 0
                    ) +
                    COALESCE(
                        CASE 
                            WHEN CAST(sub."Valor Count" AS DECIMAL(10,2)) > 1 
                            THEN CAST(m.mpa_valor_add_on_terminal AS DECIMAL(10,2))
                        END, 0
                    ) +
                    COALESCE(
                        CASE 
                            WHEN CAST(sub."Gateway Count" AS DECIMAL(10,2)) > 0 
                            THEN CAST(COALESCE(m.mpa_valor_virtual_terminal, m.mpa_valor_ecommerce) AS DECIMAL(10,2))
                        END, 0
                    )
                )
            AS DECIMAL(10,2)) AS agent_share_per_merchant

        FROM (
            SELECT 
                merchant_number,
                SUM(CASE WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma') THEN 1 ELSE 0 END) AS "Terminal Count",
                SUM(CASE WHEN LOWER(communication_type) = 'gateway' THEN 1 ELSE 0 END) AS "Gateway Count",
                SUM(CASE WHEN LOWER(terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') THEN 1 ELSE 0 END) AS "Valor Count"
            FROM zoho_orders_table
            WHERE merchant_number IS NOT NULL AND TRIM(merchant_number) <> ''
            GROUP BY merchant_number
        ) AS sub
        LEFT JOIN merchants m ON sub.merchant_number = m.merchant_number
        WHERE m.outside_agents IS NOT NULL AND TRIM(m.outside_agents) <> ''
    )

    SELECT 
        outside_agents AS "Agent",
        SUM(agent_share_per_merchant) AS "Total Agent Share"
    FROM merchant_level_report
    GROUP BY outside_agents
    ORDER BY "Total Agent Share" DESC;
""")








# Load the Equipment Report for Agents
equipment_agent_summary = load_data_from_db("""
    SELECT 
        sub.outside_agents,
        SUM(sub."Equipments Fee") AS "Total Equipments Fee"
    FROM (

        SELECT 
            m.outside_agents,

            -- Count terminals
            SUM(
                CASE 
                    WHEN LOWER(o.communication_type) IN ('wireless - gprs', 'wireless - cdma') 
                    THEN 1 ELSE 0 
                END
            ) AS "Terminal Count",

            -- Count gateways
            SUM(
                CASE 
                    WHEN LOWER(o.communication_type) = 'gateway' 
                    THEN 1 ELSE 0 
                END
            ) AS "Gateway Count",

            -- Count valor terminals
            SUM(
                CASE 
                    WHEN LOWER(o.terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') 
                    THEN 1 ELSE 0 
                END
            ) AS "Valor Count",

            -- Total Equipment Fee Calculation
            CAST(
                (
                    -- Terminal + Gateway
                    SUM(
                        CASE 
                            WHEN LOWER(o.communication_type) IN ('wireless - gprs', 'wireless - cdma', 'gateway') 
                            THEN 1 ELSE 0 
                        END
                    ) * 10
                ) +
                -- Valor Fee
                CASE
                    WHEN SUM(
                        CASE 
                            WHEN LOWER(o.terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') 
                            THEN 1 ELSE 0 
                        END
                    ) = 1 THEN 5
                    WHEN SUM(
                        CASE 
                            WHEN LOWER(o.terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') 
                            THEN 1 ELSE 0 
                        END
                    ) > 1 THEN 5 + ((SUM(
                        CASE 
                            WHEN LOWER(o.terminal_detail) IN ('vp550', 'vl100', 'vl110', 'vl100 pro') 
                            THEN 1 ELSE 0 
                        END
                    ) - 1) * 2)
                    ELSE 0
                END
            AS DECIMAL(10,2)) AS "Equipments Fee"

        FROM zoho_orders_table o
        LEFT JOIN merchants m ON o.merchant_number = m.merchant_number

        WHERE o.merchant_number IS NOT NULL AND TRIM(o.merchant_number) <> ''
        GROUP BY m.merchant_number, m.outside_agents

    ) AS sub
    WHERE sub.outside_agents IS NOT NULL 
      AND TRIM(sub.outside_agents) <> ''
      AND sub."Equipments Fee" > 0
    GROUP BY sub.outside_agents
    ORDER BY "Total Equipments Fee" DESC;
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
    

    
##############################################################################################################################################################################################    
st.markdown("\n\n")  # Extra spacing

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Count Tables", "Accounts Full Data", "Orders Full Data", "Products Full Data", "PCI Report", "Equipment Report", "Agents", "Merchants", "HW Visualization"])

# Sub-navigation for HW Visualization
if page == "HW Visualization":
    sub_page = st.sidebar.radio("Select a Visualization", ["Product Locations", "Active Agents", "Available Product"])
    
    if sub_page == "Product Locations":
        st.header("📍 Product Location Distribution")
        product_locations_data = visualization.show_visualization(sub_page)
    
    elif sub_page == "Active Agents":
        st.header("🤝 Active Agents")
        product_locations_data = visualization.show_visualization(sub_page)
        
    elif sub_page == "Available Product":
        st.header("📤 Available Product")
        product_locations_data = visualization.show_visualization(sub_page)


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
    st.header("🗃️ Orders Full Data")
    if orders_full_data is not None:
        st.dataframe(orders_full_data)
    else:
        st.warning("No data available. Run the report script first.")  

elif page == "Products Full Data":
    st.header("📦 Products Full Data")
    if products_full_data is not None:
        st.dataframe(products_full_data)
    else:
        st.warning("No data available. Run the report script first.")  

elif page == "Active Products":
    st.header("📦 Active Products")
    if products_Merchant_Location_data is not None:
        st.dataframe(products_Merchant_Location_data)
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
    st.header("🏬📋 Merchant Equipment")
    if equipment_pivot_report is not None:
        st.dataframe(equipment_pivot_report)
    else:
        st.warning("No data available for pivot table.")

    # Add a separator
    st.markdown("---")

    # Display Pivot Table Below
    st.header("🧑‍💼💰 Agent Equipment Fee")
    if equipment_agent_summary is not None:
        st.dataframe(equipment_agent_summary)
    else:
        st.warning("No data available for pivot table.")
