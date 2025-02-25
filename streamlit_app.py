import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# PostgreSQL connection details
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        return None

def fetch_zoho_accounts():
    """Fetches PCI Fee, PCI Amount, and Split from the PostgreSQL database."""
    conn = get_db_connection()
    if not conn:
        return None

    cur = conn.cursor()
    query = "SELECT pci_fee, pci_amnt, split FROM zoho_accounts"
    cur.execute(query)
    rows = cur.fetchall()
    columns = ["PCI Fee ($)", "PCI Amount ($)", "Split (%)"]
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=columns) if rows else None

# 🎨 Improved UI Layout
st.set_page_config(page_title="Annual PCI Report", layout="wide")

# **Header Section (Fix Logo & Title Alignment)**
col1, col2 = st.columns([1, 5])

# ✅ Ensure the correct logo path
logo_path = os.path.join(os.getcwd(), "logo.png")

with col1:
    if os.path.exists(logo_path):
        st.image(logo_path, width=80)  
    else:
        st.warning("⚠️ Logo not found. Please check the file path.")

with col2:
    st.markdown("<h1 style='vertical-align: middle;'>Annual PCI Report</h1>", unsafe_allow_html=True)

st.markdown("---")

# **Fetch and Display Data**
df = fetch_zoho_accounts()

if df is not None and not df.empty:
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No data found in `zoho_accounts` table. Please check your database.")

# **Refresh Button**
if st.button("🔄 Refresh Data"):
    df = fetch_zoho_accounts()
    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("⚠️ No data found in `zoho_accounts` table.")
