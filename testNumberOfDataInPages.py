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
        "client_secret": ZOHO_CLIENT_SECRET",
        "grant_type": "refresh_token"
    }
    
    response = requests.post(url, data=payload)
    data = response.json()

    if "access_token" in data:
        return data["access_token"]
    else:
        print(f"❌ Failed to refresh token: {data}")
        return None

def test_pagination():
    """Test if Zoho API is returning more than 200 records."""
    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token. Exiting.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    page = 1
    total_records = 0
    has_more_records = True

    while has_more_records:
        response = requests.get(f"{ZOHO_API_URL}?page={page}&per_page=200", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            total_records += len(records)
            
            has_more_records = data.get("info", {}).get("more_records", False)
            print(f"📄 Page {page} fetched: {len(records)} records (Total: {total_records})")

            page += 1
        else:
            print(f"❌ Failed to fetch data from Zoho CRM: {response.text}")
            has_more_records = False
        
        if not records:
            print("✅ No more records found.")
            break

test_pagination()
