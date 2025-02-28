import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Zoho API Credentials
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

def get_access_token():
    """Refreshes the Zoho OAuth access token."""
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
        print(f"❌ Error fetching access token: {data}")
        return None

if __name__ == "__main__":
    token = get_access_token()
    if token:
        print("✅ Successfully retrieved Zoho access token.")
    else:
        print("❌ Failed to retrieve access token.")
