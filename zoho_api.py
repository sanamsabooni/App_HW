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
    """Fetches PCI Fee, PCI Amount, and Split from Zoho CRM and saves it to PostgreSQL."""
    access_token = get_access_token()
    if not access_token:
        print("❌ No valid access token. Exiting.")
        return

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    response = requests.get(ZOHO_API_URL, headers=headers)

    if response.status_code == 200:
        data = response.json().get("data", [])
        save_to_database(data)
    else:
        print(f"❌ Failed to fetch data from Zoho CRM: {response.text}")

def save_to_database(data):
    """Saves fetched Zoho CRM data to PostgreSQL."""
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()

    for record in data:
        account_id = record.get("id")
        pci_fee = record.get("PCI_Fee", 0.00)
        pci_amnt = record.get("PCI_Amnt", 0.00)
        split = record.get("Split", 0.00)

        cur.execute("""
            INSERT INTO zoho_accounts (account_id, pci_fee, pci_amnt, split)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (account_id) DO UPDATE 
            SET pci_fee = EXCLUDED.pci_fee, 
                pci_amnt = EXCLUDED.pci_amnt, 
                split = EXCLUDED.split;
        """, (account_id, pci_fee, pci_amnt, split))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data successfully saved to the database.")

if __name__ == "__main__":
    fetch_zoho_data()
