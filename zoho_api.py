import os
import requests
import json
import time
import streamlit as st
import pandas as pd  # Import pandas for table
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Load environment variables
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_BASE = os.getenv("ZOHO_BASE")
ZOHO_REDIRECT_URI = os.getenv("ZOHO_REDIRECT_URI")
ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")

# File paths
TOKEN_TIMESTAMP_FILE = "refresh_token_timestamp.txt"
DATA_FILE = "zoho_accounts.json"  # File to store fetched account data

# Access token and expiration handling
access_token = None
access_token_expiration = None

def update_refresh_token_timestamp():
    with open(TOKEN_TIMESTAMP_FILE, "w") as file:
        file.write(str(time.time()))

def refresh_access_token():
    """Refresh Zoho API access token."""
    global access_token, access_token_expiration

    if not access_token or (access_token_expiration and time.time() > access_token_expiration):
        st.write("🔄 Refreshing access token...")

        url = f"{ZOHO_BASE}/oauth/v2/token"
        data = {
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "redirect_uri": ZOHO_REDIRECT_URI,
            "grant_type": "refresh_token",
        }

        response = requests.post(url, data=data)
        
        if response.status_code != 200:
            st.error(f"❌ Failed to refresh access token: {response.status_code} {response.text}")
            return None

        try:
            response_data = response.json()
            access_token = response_data["access_token"]
            expires_in = response_data["expires_in"]
            access_token_expiration = time.time() + expires_in
            update_refresh_token_timestamp()
            return access_token
        except json.JSONDecodeError:
            st.error(f"❌ Invalid JSON response when refreshing token: {response.text}")
            return None

def get_accounts(access_token):
    """Fetch account data from Zoho CRM."""
    url = f"{ZOHO_API_BASE}/crm/v2/Accounts"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        st.error(f"❌ Failed to fetch accounts: {response.status_code} {response.text}")
        return []

    try:
        accounts = response.json().get("data", [])
        save_data_to_json(accounts)  # Save locally
        return accounts
    except json.JSONDecodeError:
        st.error("❌ Invalid JSON response when fetching accounts")
        return []

def save_data_to_json(data):
    """Save the fetched account data to a local JSON file."""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        st.write(f"✅ Data saved to {DATA_FILE}")
    except Exception as e:
        st.error(f"❌ Failed to save data: {str(e)}")

def display_accounts_table(accounts):
    """Display account names, processors, and merchant numbers in a table."""
    if not accounts:
        st.write("❌ No account data available.")
        return

    table_data = []
    for account in accounts:
        account_name = account.get("Account_Name", "N/A")
        processor = account.get("Processor", "N/A")  # Ensure this field name matches Zoho CRM
        merchant_number = account.get("Merchant_Number", "N/A")  # Ensure correct field name

        # ✅ Fix: Check if processor is not None before using .lower()
        if processor and isinstance(processor, str) and processor.lower() == "fiserv":
            table_data.append({
                "Account Name": account_name,
                "Processor": processor,
                "Merchant Number": merchant_number
            })

    if not table_data:
        st.write("❌ No accounts found with Processor = Fiserv")
        return

    # Convert list to a Pandas DataFrame
    df = pd.DataFrame(table_data)

    # Display as a table in Streamlit
    st.write("## Fiserv Accounts")
    st.dataframe(df)  # Use st.table(df) for a static table

# Streamlit UI
st.title("Zoho CRM Accounts")
st.write("Click the button below to fetch and display accounts with Processor = Fiserv.")

if st.button("Fetch and Show Fiserv Accounts"):
    st.write("🔄 Fetching accounts...")
    access_token = refresh_access_token()

    if access_token:
        accounts = get_accounts(access_token)
        display_accounts_table(accounts)

