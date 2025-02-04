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
DATA_FILE = "zoho_modules.json"  # File to store fetched modules

# Access token and expiration handling
access_token = None
access_token_expiration = None

def update_refresh_token_timestamp():
    with open(TOKEN_TIMESTAMP_FILE, "w") as file:
        file.write(str(time.time()))

def refresh_access_token():
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

def get_modules(access_token):
    url = f"{ZOHO_API_BASE}/crm/v2/settings/modules"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        st.error(f"❌ Failed to fetch modules: {response.status_code} {response.text}")
        return []

    try:
        modules = response.json().get("modules", [])
        save_data_to_json(modules)  # Save data locally
        return modules
    except json.JSONDecodeError:
        st.error("❌ Invalid JSON response when fetching modules")
        return []

def save_data_to_json(data):
    """Save the fetched Zoho CRM modules data to a local JSON file."""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        st.write(f"✅ Data saved to {DATA_FILE}")
    except Exception as e:
        st.error(f"❌ Failed to save data: {str(e)}")

# Streamlit UI
st.title("Zoho CRM Modules")
st.write("Click the button below to fetch and save Zoho CRM modules.")

if st.button("Fetch and Save Modules"):
    st.write("🔄 Fetching modules...")
    access_token = refresh_access_token()
    
    if access_token:
        modules = get_modules(access_token)

        if modules:
            # Create a table data structure
            table_data = []
            
            for index, module in enumerate(modules, start=1):  # Start counting from 1
                table_data.append({"S.No": index, "Module Name": module.get("api_name", "N/A")})

            # Convert list to a Pandas DataFrame
            df = pd.DataFrame(table_data)

            # Display as a table in Streamlit with proper indexing
            st.write("## Zoho CRM Modules Table")
            st.dataframe(df.set_index("S.No"))  # Set "S.No" as the index to remove extra numbers

        else:
            st.write("❌ No modules found.")

