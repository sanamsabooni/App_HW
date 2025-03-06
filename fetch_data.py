import requests
import psycopg2
import os
from dotenv import load_dotenv
from zoho_api import get_access_token  # ✅ Correct function name
import ace_tools


# Load environment variables
load_dotenv()

# Database connection details
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
API_URL = "https://www.zohoapis.com/crm/v2/Accounts"

def clean_value(value):
    """Ensure values are strings and extract 'display_value' if value is a dictionary."""
    if isinstance(value, dict):
        return value.get("display_value", "")  # Extract display_value if exists
    return str(value) if value else None  # Convert to string or None

def fetch_and_store_data():
    """Fetch all paginated data from Zoho API and store it in PostgreSQL."""
    access_token = get_access_token()
    if not access_token:
        print("❌ Failed to get access token. Exiting.")
        return

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
                account_id = clean_value(account.get("id"))
                account_number = clean_value(account.get("account_number"))
                partner_name = clean_value(account.get("Partner_Name"))
                office_code = clean_value(account.get("Office_Code"))
                office_code_2 = clean_value(account.get("Office_Code_2"))
                split = clean_value(account.get("Split"))
                split_2 = clean_value(account.get("Split_2"))
                pci_fee = clean_value(account.get("PCI_Fee"))
                sales_id = clean_value(account.get("Sales_ID"))
                pci_amnt = clean_value(account.get("PCI_Amnt"))
                merchant_number = clean_value(account.get("Merchant_Number"))                
                account_name = clean_value(account.get("Account_Name"))
                outside_agent = clean_value(account.get("Outside_Agent"))
                date_approved = clean_value(account.get("Date_Approved"))
                layout = clean_value(account.get("Layout"))

                # ✅ Insert into zoho_accounts_table (All Records)
                cur.execute("""
                    INSERT INTO zoho_accounts_table (account_number, account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, merchant_number, sales_id, pci_amnt, account_name, outside_agent, date_approved, layout)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_number) DO UPDATE 
                    SET account_id = EXCLUDED.account_id,
                        partner_name = EXCLUDED.partner_name, 
                        office_code = EXCLUDED.office_code, 
                        office_code_2 = EXCLUDED.office_code_2,
                        split = EXCLUDED.split, 
                        split_2 = EXCLUDED.split_2, 
                        pci_fee = EXCLUDED.pci_fee, 
                        merchant_number = EXCLUDED.merchant_number, 
                        sales_id = EXCLUDED.sales_id, 
                        pci_amnt = EXCLUDED.pci_amnt, 
                        account_name = EXCLUDED.account_name, 
                        outside_agent = EXCLUDED.outside_agent, 
                        date_approved = EXCLUDED.date_approved, 
                        layout = EXCLUDED.layout;
                """, (account_number, account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, merchant_number, sales_id, pci_amnt, account_name, outside_agent, date_approved, layout))

                # ✅ Insert into Agents Table
                if split:
                    cur.execute("""
                        INSERT INTO agents (account_number, partner_name, office_code, office_code_2, split, split_2, pci_fee, account_name, layout)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_number) DO UPDATE 
                        SET partner_name = EXCLUDED.partner_name, 
                            office_code = EXCLUDED.office_code,
                            office_code_2 = EXCLUDED.office_code_2,
                            split = EXCLUDED.split, 
                            split_2 = EXCLUDED.split_2, 
                            pci_fee = EXCLUDED.pci_fee,
                            account_name = EXCLUDED.account_name,
                            layout = EXCLUDED.layout;
                    """, (account_number, partner_name, office_code, office_code_2, split, split_2, pci_fee, account_name, layout))



                # ✅ Insert into Merchants Table
                if merchant_number:
                    cur.execute("""
                        INSERT INTO merchants (account_number, merchant_number, account_name, sales_id, outside_agent, pci_amnt, date_approved, layout)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_number) DO UPDATE 
                        SET merchant_number = EXCLUDED.merchant_number,
                            account_name = EXCLUDED.account_name, 
                            sales_id = EXCLUDED.sales_id, 
                            outside_agent = EXCLUDED.outside_agent, 
                            pci_amnt = EXCLUDED.pci_amnt,
                            date_approved = EXCLUDED.date_approved,
                            layout = EXCLUDED.layout;
                    """, (account_number, merchant_number, account_name, sales_id, outside_agent, pci_amnt, date_approved, layout))

            conn.commit()
            print(f"📢 Page {page}: Inserted {len(records)} records. Total so far: {total_records}")

            page += 1  # Move to the next page
        else:
            print(f"❌ Error fetching page {page}: {response.text}")
            break

    cur.close()
    conn.close()
    print(f"✅ Total records inserted: {total_records}")

if __name__ == "__main__":
    fetch_and_store_data()
