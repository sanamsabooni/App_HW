import os
import requests
import json
import time
from dotenv import load_dotenv
from utils.zoho_utils import refresh_access_token
from utils.db_utils import save_contacts_to_db

# Load environment variables
load_dotenv()

ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")

def get_contacts():
    """Fetch all contacts from Zoho CRM with pagination."""
    access_token = refresh_access_token()
    if not access_token:
        print("❌ Failed to get access token.")
        return []

    all_contacts = []
    page = 1
    more_records = True

    while more_records:
        url = f"{ZOHO_API_BASE}/crm/v2/Contacts?page={page}&per_page=200"
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ Failed to fetch contacts: {response.status_code} {response.text}")
            return []

        data = response.json()
        contacts = data.get("data", [])
        all_contacts.extend(contacts)

        more_records = data.get("info", {}).get("more_records", False)
        page += 1

    save_contacts_to_db(all_contacts)
    return all_contacts

if __name__ == "__main__":
    contacts = get_contacts()
    print(f"✅ {len(contacts)} contacts fetched and saved to the database.")
