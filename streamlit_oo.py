import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection  # Ensure this function is correctly implemented
import matplotlib.pyplot as plt
import visualization  # Import visualization for HW Visualization
from merchant_chatbot import run_chatbot

from pci_report_oo import PCIReport
from equipment_oo import EquipmentReport

from commission_report_oo import CommissionReport

from equipment_oo import EquipmentReport

from tables_oo import TableStats

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
tables = TableStats()

count_tables = tables.load_table_counts()
accounts_full_data = tables.load_accounts_full()


# Orders Full Data Query
orders_full_data = load_data_from_db("SELECT * FROM zoho_orders_table;")

# Products Full Data Query
products_full_data = load_data_from_db("SELECT * FROM zoho_products_table;")

# Products at Merchant Location Data Query
products_Merchant_Location_data = load_data_from_db("SELECT * FROM products_at_merchants_table;")

# Agents & Merchants Queries
agents_data = load_data_from_db("SELECT * FROM agents;")
merchants_data = products_full_data = load_data_from_db("SELECT * FROM merchants WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';")


# 📄 PCI Report
pci = PCIReport()
pci_report = pci.load_data()

# Clean-up (optional)
if pci_report is not None:
    pci_report = pci_report.drop(columns=['pci_difference'], errors='ignore')

st.subheader("📄 PCI Report")
if pci_report is not None and not pci_report.empty:
    st.dataframe(pci_report)
else:
    st.info("No PCI report data available.")

# 🔧 Equipment Report
equipment = EquipmentReport()

# Equipment Report (raw order detail + counts)
equipment_report = equipment.load_main_report()
st.subheader("🔧 Equipment Report")
if equipment_report is not None and not equipment_report.empty:
    st.dataframe(equipment_report)
else:
    st.info("No equipment report data available.")

# Equipment Pivot Report (Merchant view)
equipment_pivot_report = equipment.load_pivot_report()
st.subheader("📊 Equipment Pivot Report (Merchant View)")
if equipment_pivot_report is not None and not equipment_pivot_report.empty:
    st.dataframe(equipment_pivot_report)
else:
    st.info("No pivot report data available.")

# Equipment Agent Charges
equipment_agent_charges = equipment.load_agent_charges()
st.subheader("💼 Equipment Agent Charges")
if equipment_agent_charges is not None and not equipment_agent_charges.empty:
    st.dataframe(equipment_agent_charges)
else:
    st.info("No agent charge data available.")

# Equipment Agent Summary
equipment_agent_summary = equipment.load_agent_summary()
st.subheader("📋 Equipment Agent Summary")
if equipment_agent_summary is not None and not equipment_agent_summary.empty:
    st.dataframe(equipment_agent_summary)
else:
    st.info("No agent summary data available.")


commission = CommissionReport()

# 💸 Full Commission Report
commission_df = commission.load_full_report()
st.subheader("💸 Commission Report")
if commission_df is not None and not commission_df.empty:
    st.dataframe(commission_df)
else:
    st.info("No commission report data available.")

# ⚠️ Pending Clawbacks
pending_df = commission.load_closed_pending_clawback()
st.subheader("⚠️ Closed Accounts with Pending Clawbacks")
if pending_df is not None and not pending_df.empty:
    st.dataframe(pending_df)
else:
    st.info("No pending clawback records found.")

 
##############################################################################################################################################################################################    
st.markdown("\n\n")  # Extra spacing

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Count Tables", "Accounts Full Data", "Orders Full Data", "Products Full Data", "PCI Report", "Equipment Report", "Commission Report", "Agents", "Merchants", "ChatBot" , "HW Visualization"])


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

    # Add a separator
    st.markdown("---")

elif page == "Commission Report":
    st.header("🧾 Commission Report")
    if commission_df is not None and not commission_df.empty:
        st.dataframe(commission_df)
    else:
        st.warning("No data available. Run the report script first.")

    # Add a separator
    st.markdown("---")

    st.header("📌 Closed Commissions Pending Clawback")
    if pending_df is not None and not pending_df.empty:
        st.dataframe(pending_df)
    else:
        st.warning("No data available for closed commissions with pending clawback.")


elif page == "ChatBot":
    sub_page = st.header("🤖 HubWallet ChatBot")
    product_locations_data = run_chatbot()


# Sub-navigation for HW Visualization
elif page == "HW Visualization":
    sub_page = st.sidebar.radio("Select a Visualization", ["Product Locations", "Active Agents", "Available Product"])
    
    if sub_page == "Product Locations":
        st.header("📍 Product Location Distribution")
        product_locations_data = visualization.show_visualization(sub_page)
    
    elif sub_page == "Active Agents":
        st.header("🤝 Active Agents")
        active_agent = visualization.show_visualization(sub_page)
        
    elif sub_page == "Available Product":
        st.header("📤 Available Product")
        available_product = visualization.show_visualization(sub_page)
