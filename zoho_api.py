import os
import psycopg2
import requests
from dotenv import load_dotenv

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
        print(f"❌ Failed to refresh token: {data}")
        return None

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def fetch_zoho_data():
    """Fetches ALL paginated data from Zoho CRM and saves it to PostgreSQL."""
    print("🚀 Fetching data from Zoho API...")

    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token. Exiting.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    all_data = []
    page = 1
    has_more_records = True

    while has_more_records:
        url = f"{ZOHO_API_URL}?page={page}&per_page=200"
        print(f"📡 Requesting: {url}")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            page_info = data.get("info", {})

            print(f"📝 Page {page} Response Info: {page_info}")  # Print page details
            print(f"✅ Page {page}: {len(records)} records fetched.")

            if records:
                all_data.extend(records)
                page += 1  # Move to next page
                has_more_records = page_info.get("more_records", False)  # Keep going if true

            else:
                print("✅ No more records found.")
                has_more_records = False  # Stop fetching
                break
        else:
            print(f"❌ API Request Failed: {response.text}")
            break

    print(f"📊 Total records fetched: {len(all_data)}")
    save_to_database(all_data)

def save_to_database(data):
    """Saves retrieved data to PostgreSQL."""
    if not data:
        print("⚠️ No data to save.")
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    for record in data:
        cursor.execute("""
            INSERT INTO zoho_accounts (account_id, pci_fee, pci_amnt, split) 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (account_id) DO UPDATE 
            SET pci_fee = EXCLUDED.pci_fee, pci_amnt = EXCLUDED.pci_amnt, split = EXCLUDED.split
        """, (record.get("id"), record.get("PCI_Fee"), record.get("PCI_Amount"), record.get("Split")))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Successfully saved {len(data)} records to the database.")

if __name__ == "__main__":
    print("🚀 Running zoho_api.py...")
    fetch_zoho_data()
