import streamlit as st
import pandas as pd
import os

# Page Configuration
st.set_page_config(page_title="HubWallet Reports", layout="wide")

# Load Data
@st.cache_data
def load_data(filename):
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return None

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Count Tables", "Full Data", "PCI Report"])

# Load Reports
count_tables = load_data("count_tables.csv")
full_data = load_data("full_data.csv")
pci_report = load_data("pci_report.csv")

# Display Selected Page
st.title("HubWallet Reports Dashboard")

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

# Refresh Button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

st.sidebar.success("Use the navigation to view different reports.")
