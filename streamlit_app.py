import streamlit as st
import pandas as pd
import os

# Page Configuration
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
page = st.sidebar.radio("Go to", ["Count Tables", "Full Data", "PCI Report"])

# Load Data
@st.cache_data
def load_data(filename):
    if os.path.exists(filename):
        return pd.read_csv(filename, dtype={'merchant_number': str})  # Ensure text format for merchant_number
    else:
        return None

# Load Reports
count_tables = load_data("count_tables.csv")
full_data = load_data("full_data.csv")
pci_report = load_data("pci_report.csv")

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

# Refresh Button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

st.sidebar.success("Use the navigation to view different reports.")
