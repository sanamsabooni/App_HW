import streamlit as st
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Zoho API Credentials
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_URL = "https://www.zohoapis.com/crm/v2/Accounts"

# PostgreSQL connection details
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        print("Database connecting ...")
        print(f"host: {DB_HOST}")
        print(f"db: {DB_NAME}")
        print(f"user: {DB_USER}")
        print(f"pass: {DB_PASSWORD}")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        st.info("Database connected.")
        print("Database connected.")
        return conn
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        print(f"❌ Database connection error: {e}")
        return None

def fetch_zoho_accounts():
    """Fetches PCI Fee, PCI Amount, and Split from the PostgreSQL database."""
    conn = get_db_connection()
    if not conn:
        return None

    cur = conn.cursor()
    query = "SELECT account_id, pci_fee, pci_amnt, split FROM zoho_accounts"
    cur.execute(query)
    rows = cur.fetchall()
    columns = ["Account ID","PCI Fee ($)", "PCI Amount ($)", "Split (%)"]
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=columns) if rows else None

def get_access_token():
    """Refreshes the Zoho access token."""
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    
    response = requests.post(url, data=payload)
    data = response.json()

    if "access_token" in data:
        return data["access_token"]
    else:
        print(f"❌ Failed to refresh token: {data}")
        return None

def fetch_zoho_data():
    """Fetches PCI Fee, PCI Amount, and Split from Zoho CRM and saves it to PostgreSQL."""
    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token. Exiting.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(ZOHO_API_URL, headers=headers)

    if response.status_code == 200:
        data = response.json().get("data", [])
        save_to_database(data)
    else:
        print(f"❌ Failed to fetch data from Zoho CRM: {response.text}")


def save_to_database(data):
    """Saves fetched Zoho CRM data to PostgreSQL."""
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()

    for record in data:
        account_id = record.get("id")
        pci_fee = record.get("PCI_Fee", 0.00)
        pci_amnt = record.get("PCI_Amnt", 0.00)
        split = record.get("Split", 0.00)

        cur.execute("""
            INSERT INTO zoho_accounts (account_id, pci_fee, pci_amnt, split)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (account_id) DO UPDATE 
            SET pci_fee = EXCLUDED.pci_fee, 
                pci_amnt = EXCLUDED.pci_amnt, 
                split = EXCLUDED.split;
        """, (account_id, pci_fee, pci_amnt, split))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data successfully saved to the database.")


print("Start.")
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

print("fetch_zoho_data")
# fetch_zoho_data()

print("fetch_zoho_accounts.")
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
