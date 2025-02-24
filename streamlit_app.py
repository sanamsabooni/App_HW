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

# Function to connect to the database
def get_db_connection():
    """Connect to PostgreSQL and return a connection object."""
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

# Fetch table data
def fetch_zoho_accounts():
    """Fetches the Zoho Accounts table from PostgreSQL."""
    conn = get_db_connection()
    if not conn:
        return None

    cur = conn.cursor()
    cur.execute("SELECT * FROM zoho_accounts")
    rows = cur.fetchall()
    columns = ["Account ID", "Layout", "PCI Fee", "PCI Amount", "Split Percentage"]
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=columns)

# Streamlit UI
st.title("📊 Zoho Accounts Data Viewer")

# Fetch and display data
df = fetch_zoho_accounts()
if df is not None and not df.empty:
    st.dataframe(df, use_container_width=True)
else:
    st.warning("No data found in `zoho_accounts` table.")

# Refresh Button
if st.button("🔄 Refresh Data"):
    df = fetch_zoho_accounts()
    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No data found in `zoho_accounts` table.")
