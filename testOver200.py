import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Zoho API Credentials
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_URL = "https://www.zohoapis.com/crm/v2/Accounts"

def get_access_token():
    """Fetch new access token from Zoho API."""
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
        print(f"❌ Failed to fetch token: {data}")
        return None

# Fetch and print API response info section
def check_pagination_info():
    """Fetch Zoho API response and print pagination info."""
    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    url = f"{ZOHO_API_URL}?per_page=200"
    
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print("📡 API Response Info Section:")
        print(data.get("info", {}))  # Print only the "info" section
    else:
        print(f"❌ API Request Failed: {response.text}")

check_pagination_info()
