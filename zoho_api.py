import os
import requests
import json
import time
import streamlit as st
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

# File to store the last refresh token usage time
TOKEN_TIMESTAMP_FILE = "refresh_token_timestamp.txt"

# Access token and expiration handling
access_token = None
access_token_expiration = None

# Function to store the last refresh token timestamp
def get_refresh_token_timestamp():
    if os.path.exists(TOKEN_TIMESTAMP_FILE):
        with open(TOKEN_TIMESTAMP_FILE, "r") as file:
            return float(file.read().strip())
    return None

def update_refresh_token_timestamp():
    with open(TOKEN_TIMESTAMP_FILE, "w") as file:
        file.write(str(time.time()))

def is_refresh_token_expired():
    last_used = get_refresh_token_timestamp()
    if last_used:
        if time.time() - last_used > 6 * 30 * 24 * 60 * 60:  # 6 months in seconds
            st.write("❌ Refresh token expired! Re-authentication needed.")
            return True
    return False

# Function to refresh the access token
def refresh_access_token():
    global access_token, access_token_expiration

    # If the refresh token expired, re-authenticate (you need to go through the OAuth flow)
    if is_refresh_token_expired():
        st.write("🔄 Re-authentication needed. Please authorize the app.")
        return None

    # Check if the access token has expired
    if not access_token or (access_token_expiration and time.time() > access_token_expiration):
        st.write(f"🔄 Refreshing access token...")

        url = f"{ZOHO_BASE}/oauth/v2/token"
        data = {
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "redirect_uri": ZOHO_REDIRECT_URI,
            "grant_type": "refresh_token",
        }

        response = requests.post(url, data=data)
        
        st.write(f"🔹 Response Status Code: {response.status_code}")
        st.write(f"🔹 Response Text: {response.text}")

        if response.status_code != 200:
            st.error(f"❌ Failed to refresh access token: {response.status_code} {response.text}")
            return None

        try:
            response_data = response.json()
            access_token = response_data["access_token"]
            expires_in = response_data["expires_in"]  # Usually 3600 (1 hour)
            access_token_expiration = time.time() + expires_in  # Set expiration time for 1 hour
            st.write(f"✅ Access token refreshed! Expires at {access_token_expiration}")
            update_refresh_token_timestamp()  # Update the timestamp after refreshing the token
            return access_token
        except json.JSONDecodeError:
            st.error(f"❌ Invalid JSON response when refreshing token: {response.text}")
            return None


# Function to fetch Zoho CRM modules
def get_modules(access_token):
    st.write(access_token)
    url = f"{ZOHO_API_BASE}/crm/v2/settings/modules"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        st.error(f"❌ Failed to fetch modules: {response.status_code} {response.text}")
        return []

    try:
        return response.json().get("modules", [])
    except json.JSONDecodeError:
        st.error("❌ Invalid JSON response when fetching modules")
        return []

# Streamlit UI
st.title("Zoho CRM Modules")
st.write("Click the button below to fetch Zoho CRM modules.")

if st.button("Show Modules"):
    st.write("🔄 Fetching modules...")  # Show loading message
    access_token = refresh_access_token()
    
    if access_token:
        modules = get_modules(access_token)
        if modules:
            st.write("## Module Names:")
            for module in modules:
                st.write(f"✅ {module.get('api_name')}")
        else:
            st.write("❌ No modules found.")
