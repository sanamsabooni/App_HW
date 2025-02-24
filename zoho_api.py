import os
import requests
import json
from dotenv import load_dotenv
from utils.zoho_utils import refresh_access_token
from utils.db_utils import save_accounts_to_db


# Load environment variables
load_dotenv()

ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")

def extract_percentage(text):
    """Extracts percentage from text, e.g., 'Split: 70%' → 70.0"""
    import re
    match = re.search(r"(\d+(\.\d+)?)%", text)
    return float(match.group(1)) if match else None

def get_accounts():
    """Fetch all accounts from Zoho CRM with required fields."""
    access_token = refresh_access_token()
    if not access_token:
        print("❌ Failed to get access token.")
        return []

    all_accounts = []
    page = 1
    more_records = True

    while more_records:
        url = f"{ZOHO_API_BASE}/crm/v2/Accounts?page={page}&per_page=200"
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ Failed to fetch accounts: {response.status_code} {response.text}")
            return []

        data = response.json()
        accounts = data.get("data", [])
        
        for account in accounts:
            layout = account.get("Layout", {}).get("name", "")
            account_id = account.get("id")

            pci_fee = None
            pci_amnt = None
            split_percentage = None

            if layout == "Agent":
                pci_fee = account.get("Schedule_Fees", {}).get("PCI Fee")
                split_text = account.get("Schedule_A_Fees", {}).get("Split", "")
                split_percentage = extract_percentage(split_text)

            elif layout == "Standard":
                pci_amnt = account.get("Fee_Details", {}).get("PCI Amnt")

            all_accounts.append((account_id, layout, pci_fee, pci_amnt, split_percentage))

        more_records = data.get("info", {}).get("more_records", False)
        page += 1

    return all_accounts

def get_contacts():
    """Fetch all contacts from Zoho CRM."""
    access_token = refresh_access_token()
    if not access_token:
        print("❌ Failed to get access token.")
        return []

    url = f"{ZOHO_API_BASE}/crm/v2/Contacts"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to fetch contacts: {response.status_code} {response.text}")
        return []

    data = response.json()
    return data.get("data", [])


if __name__ == "__main__":
    accounts_data = get_accounts()
    if accounts_data:
        save_accounts_to_db(accounts_data)

