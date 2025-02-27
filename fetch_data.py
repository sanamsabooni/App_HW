import requests
import psycopg2
import os
from dotenv import load_dotenv
from zoho_api import get_access_token  # Import authentication

load_dotenv()

# Database connection details
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
API_URL = "https://www.zohoapis.com/crm/v2/Accounts"

def fetch_and_store_data():
    """Fetch all paginated data from Zoho API and store it in PostgreSQL."""
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()

    page = 1
    total_records = 0

    while True:
        response = requests.get(f"{API_URL}?page={page}&per_page=200", headers=headers)

        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            total_records += len(records)

            if not records:
                break  # No more records, stop fetching

            for account in records:
                partner_name = account.get("Partner_Name", None)
                office_code = account.get("Office_Code", None)
                office_code_2 = account.get("Office_Code_2", None)
                split = account.get("Split", None)
                pci_fee = account.get("PCI_Fee", None)
                sales_id = account.get("Sales_ID", None)
                pci_amnt = account.get("PCI_Amnt", None)
                account_name = account.get("Account_Name", None)
                outside_agent = account.get("Outside_Agent", None)

                insert_query = """
                INSERT INTO accounts (partner_name, office_code, office_code_2, split, pci_fee, sales_id, pci_amnt, account_name, outside_agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sales_id) DO UPDATE SET 
                    partner_name = EXCLUDED.partner_name,
                    office_code = EXCLUDED.office_code,
                    office_code_2 = EXCLUDED.office_code_2,
                    split = EXCLUDED.split,
                    pci_fee = EXCLUDED.pci_fee,
                    pci_amnt = EXCLUDED.pci_amnt,
                    account_name = EXCLUDED.account_name,
                    outside_agent = EXCLUDED.outside_agent;
                """

                cur.execute(insert_query, (partner_name, office_code, office_code_2, split, pci_fee, sales_id, pci_amnt, account_name, outside_agent))

            conn.commit()
            print(f"Page {page}: Inserted {len(records)} records. Total so far: {total_records}")

            page += 1  # Move to the next page
        else:
            print(f"Error fetching page {page}: {response.text}")
            break

    cur.close()
    conn.close()
    print(f"✅ Total records inserted: {total_records}")

if __name__ == "__main__":
    fetch_and_store_data()
