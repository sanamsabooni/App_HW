#test
import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Zoho API Credentials
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_URL = "https://www.zohoapis.com/crm/v2/Accounts"

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

def fetch_test_data():
    """Fetches a sample record from Zoho CRM."""
    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token. Exiting.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(ZOHO_API_URL, headers=headers)

    if response.status_code == 200:
        data = response.json().get("data", [])
        if data:
            print("✅ Successfully fetched data from Zoho CRM!")
            print("🔹 Sample Record:")
            print(data[0])  # Print the first record
        else:
            print("⚠️ No records found in Zoho CRM.")
    else:
        print(f"❌ Failed to fetch data: {response.text}")

if __name__ == "__main__":
    fetch_test_data()
