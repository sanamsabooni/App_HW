import requests
import psycopg2
import os
from dotenv import load_dotenv
from zoho_api import get_access_token  # ✅ Correct function name
import re


# Load environment variables
load_dotenv()

# Database connection details
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

# Define API URLs for each module (FIXED module name for Orders)
API_ENDPOINTS = {
    "Accounts": "https://www.zohoapis.com/crm/v2/Accounts",
    "Sales_Orders": "https://www.zohoapis.com/crm/v2/Sales_Orders",
    "Products": "https://www.zohoapis.com/crm/v2/Products"
}

def clean_value(value):
    """Ensure values are strings and extract 'display_value' if value is a dictionary."""
    if isinstance(value, dict):
        return value.get("display_value", "")  # Extract display_value if exists
    return str(value) if value else None  # Convert to string or None

def fetch_accounts_data(conn, cur, headers):
    """Fetch and insert data for the Accounts module."""
    API_URL = API_ENDPOINTS["Accounts"]
    page = 1
    total_records = 0
    module = "Accounts"

    print(f"\n📢 Fetching data from {module}...\n")

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
                partner_name = clean_value(account.get("Partner_Name"))
                office_code = clean_value(account.get("Office_Code"))
                office_code_2 = clean_value(account.get("Office_Code_2"))
                split = clean_value(account.get("Split"))
                split_2 = clean_value(account.get("Split_2"))
                pci_fee = clean_value(account.get("PCI_Fee"))
                merchant_number = clean_value(account.get("Merchant_Number")) 
                account_status = clean_value(account.get("Account_Status")) 
                sales_id = clean_value(account.get("Sales_ID"))
                outside_agents = clean_value(account.get("Outside_Agents"))
                pci_amnt = clean_value(account.get("PCI_Amnt"))    
                account_name = clean_value(account.get("Account_Name"))
                date_approved = clean_value(account.get("Date_Approved"))
                mpa_wireless_fee = clean_value(account.get("MPA_Wireless_Fee"))
                mpa_valor_portal_access = clean_value(account.get("MPA_Valor_Portal_Access"))
                mpa_valor_add_on_terminal = clean_value(account.get("MPA_Valor_Portal_Access_on_Add_on_Terminal"))
                mpa_valor_virtual_terminal = clean_value(account.get("MPA_Valor_Virtual_Terminal"))
                mpa_valor_ecommerce = clean_value(account.get("MPA_Valor_eCommerce"))
                processor = clean_value(account.get("Processor"))
                approved = clean_value(account.get("Approved"))
                commission_amount = clean_value(account.get("Commission_Amount"))
                commission_pay_date = clean_value(account.get("Commission_Pay_Date"))
                paid = clean_value(account.get("Paid"))
                clawback = clean_value(account.get("ClawBack"))
                clawback_date = clean_value(account.get("ClawBack_Date"))


                # ✅ Insert into zoho_accounts_table (All Records)
                if merchant_number:
                    cur.execute("""
                        INSERT INTO zoho_accounts_table (account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, merchant_number, account_status, sales_id, outside_agents, pci_amnt, account_name, date_approved, mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal, mpa_valor_virtual_terminal, mpa_valor_ecommerce, processor, approved, commission_amount, commission_pay_date, paid, clawback, clawback_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_id) DO UPDATE SET
                            partner_name = EXCLUDED.partner_name, 
                            office_code = EXCLUDED.office_code, 
                            office_code_2 = EXCLUDED.office_code_2,
                            split = EXCLUDED.split, 
                            split_2 = EXCLUDED.split_2, 
                            pci_fee = EXCLUDED.pci_fee, 
                            merchant_number = EXCLUDED.merchant_number, 
                            account_status = EXCLUDED.account_status,
                            sales_id = EXCLUDED.sales_id, 
                            outside_agents = EXCLUDED.outside_agents,
                            pci_amnt = EXCLUDED.pci_amnt, 
                            account_name = EXCLUDED.account_name, 
                            date_approved = EXCLUDED.date_approved,
                            mpa_wireless_fee = EXCLUDED.mpa_wireless_fee,
                            mpa_valor_portal_access = EXCLUDED.mpa_valor_portal_access,
                            mpa_valor_add_on_terminal = EXCLUDED.mpa_valor_add_on_terminal,
                            mpa_valor_virtual_terminal = EXCLUDED.mpa_valor_virtual_terminal,
                            mpa_valor_ecommerce = EXCLUDED.mpa_valor_ecommerce,
                            processor = EXCLUDED.processor,
                            approved = EXCLUDED.approved,
                            commission_amount = EXCLUDED.commission_amount,
                            commission_pay_date = EXCLUDED.commission_pay_date,
                            paid = EXCLUDED.paid,
                            clawback = EXCLUDED.clawback,
                            clawback_date = EXCLUDED.clawback_date;
                    """, (account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, merchant_number, account_status, sales_id, outside_agents, pci_amnt, account_name, date_approved, mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal, mpa_valor_virtual_terminal, mpa_valor_ecommerce, processor, approved, commission_amount, commission_pay_date, paid, clawback, clawback_date))
            
                # ✅ Insert into Agents Table
                if split:
                    cur.execute("""
                        INSERT INTO agents (account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, account_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_id) DO UPDATE 
                        SET partner_name = EXCLUDED.partner_name, 
                            office_code = EXCLUDED.office_code,
                            office_code_2 = EXCLUDED.office_code_2,
                            split = EXCLUDED.split, 
                            split_2 = EXCLUDED.split_2, 
                            pci_fee = EXCLUDED.pci_fee,
                            account_name = EXCLUDED.account_name;
                    """, (account_id, partner_name, office_code, office_code_2, split, split_2, pci_fee, account_name))


                # ✅ Insert into Merchants Table
                if merchant_number and outside_agents:
                    cur.execute("""
                        INSERT INTO merchants (account_id, merchant_number, account_name, account_status, sales_id, outside_agents, pci_amnt, date_approved, mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal, mpa_valor_virtual_terminal, mpa_valor_ecommerce, processor, approved, commission_amount, commission_pay_date, paid, clawback, clawback_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_id) DO UPDATE 
                        SET merchant_number = EXCLUDED.merchant_number,
                            account_name = EXCLUDED.account_name,
                            account_status =  EXCLUDED.account_status,
                            sales_id = EXCLUDED.sales_id,  
                            outside_agents = EXCLUDED.outside_agents,
                            pci_amnt = EXCLUDED.pci_amnt,
                            date_approved = EXCLUDED.date_approved,
                            mpa_wireless_fee = EXCLUDED.mpa_wireless_fee,
                            mpa_valor_portal_access = EXCLUDED.mpa_valor_portal_access,
                            mpa_valor_add_on_terminal = EXCLUDED.mpa_valor_add_on_terminal,
                            mpa_valor_virtual_terminal = EXCLUDED.mpa_valor_virtual_terminal,
                            mpa_valor_ecommerce = EXCLUDED.mpa_valor_ecommerce,
                            processor = EXCLUDED.processor,
                            approved = EXCLUDED.approved,
                            commission_amount = EXCLUDED.commission_amount,
                            commission_pay_date = EXCLUDED.commission_pay_date,
                            paid = EXCLUDED.paid,
                            clawback = EXCLUDED.clawback,
                            clawback_date = EXCLUDED.clawback_date;
                    """, (account_id, merchant_number, account_name, account_status, sales_id, outside_agents, pci_amnt, date_approved, mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal, mpa_valor_virtual_terminal, mpa_valor_ecommerce, processor, approved, commission_amount, commission_pay_date, paid, clawback, clawback_date))


            conn.commit()
            print(f"📢 {module} - Page {page}: Inserted {len(records)} records. Total so far: {total_records}")

            page += 1
        else:
            print("✅ All data has been retrieved successfully. No more records to fetch.")
            break

    print(f"✅ Total {module} records inserted: {total_records}")

    

def clean_numeric(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)

    val = str(val).strip()
    val = re.sub(r'[$,%]', '', val)  # remove $ and %

    if val == "":
        return None

    try:
        return float(val)
    except Exception:
        return None


def generate_and_insert_pci_report(conn, cur):
    """Generate PCI report by joining merchants, agents, and zoho_accounts_table properly."""
    print("\n📢 Generating PCI Report from merchants + agents + zoho_accounts_table...\n")

    cur.execute("""
        SELECT 
            m.merchant_number,
            m.account_name,
            m.sales_id,
            a.partner_name AS agent_name,
            m.date_approved,
            z.pci_fee,
            m.pci_amnt,
            a.split
        FROM merchants m
        LEFT JOIN zoho_accounts_table z ON m.merchant_number = z.merchant_number
        LEFT JOIN agents a ON m.account_name = a.account_name
        WHERE m.merchant_number IS NOT NULL
    """)

    rows = cur.fetchall()
    total_records = 0
    skipped = 0
    debug_limit = 10

    for row in rows:
        merchant_number, account_name, sales_id, agent_name, date_approved, pci_fee, pci_amnt, split_value = row

        raw_pci_fee = pci_fee
        raw_pci_amnt = pci_amnt
        raw_split = split_value

        pci_fee = clean_numeric(pci_fee)
        pci_amnt = clean_numeric(pci_amnt)
        split_value = clean_numeric(split_value)

        if pci_fee is None or pci_amnt is None or split_value is None:
            skipped += 1
            if debug_limit > 0:
                print(f"⚠️ Skipped: merchant={merchant_number}, pci_fee='{raw_pci_fee}', pci_amnt='{raw_pci_amnt}', split='{raw_split}'")
                debug_limit -= 1
            continue

        pci_share = pci_amnt * split_value
        max_share = pci_fee / 2
        approval_month = date_approved.strftime("%Y-%m") if date_approved else None
        effective_month = approval_month

        cur.execute("""
            INSERT INTO pci_report_table (
                merchant_number, account_name, sales_id, agent_name,
                approval_month, effective_month, pci_fee, pci_amnt,
                split_value, pci_share, max_share
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (
            merchant_number, account_name, sales_id, agent_name,
            approval_month, effective_month, pci_fee, pci_amnt,
            split_value, pci_share, max_share
        ))

        total_records += 1

    conn.commit()
    print(f"✅ {total_records} records inserted into pci_report_table.")
    print(f"⚠️ Skipped {skipped} rows due to missing or invalid pci_fee, pci_amnt, or split.")


def fetch_orders_data(conn, cur, headers):
    """Fetch and insert data for the Sales_Orders module with unique order_id."""
    API_URL = API_ENDPOINTS["Sales_Orders"]
    page = 1
    total_records = 0
    module = "Sales_Orders"

    print(f"\n📢 Fetching data from {module}...\n")

    # Retrieve the last used order_id to continue counting
    cur.execute("SELECT MAX(order_id) FROM zoho_orders_table;")
    last_order_id = cur.fetchone()[0]
    last_order_id = last_order_id if last_order_id else 0  # Start from 0 if empty

    while True:
        response = requests.get(f"{API_URL}?page={page}&per_page=200", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            total_records += len(records)

            if not records:
                break  # No more records, stop fetching

            for record in records:
                last_order_id += 1  # Increment order_id counter
                order_id = last_order_id  # Assign new unique ID

                so_number = clean_value(record.get("SO_Number"))
                merchant_number = clean_value(record.get("Merchant_Number"))
                tech_setup_order_options = clean_value(record.get("Tech_Setup_Order_Options"))
                communication_type = clean_value(record.get("Communication_Type"))
                wireless_carrier = clean_value(record.get("Wireless_Carrier"))
                terminal_detail = clean_value(record.get("Terminal_Detail"))
                terminal_id = clean_value(record.get("Terminal_ID"))
                outside_agents = clean_value(record.get("Outside_Agents"))
                status = clean_value(record.get("Status"))
                est_equip_due_date = clean_value(record.get("Est_equip_due_date"))
                equipment_received_date = clean_value(record.get("Equipment_Received_Date"))
                tracking_number = clean_value(record.get("Tracking_Number"))
                tracking_number2 = clean_value(record.get("Tracking_Number2"))
                purchase_settled = clean_value(record.get("Purchase_Settled"))
                date_shipped = clean_value(record.get("Date_Shipped"))
                location = clean_value(record.get("Location"))
                subject = clean_value(record.get("Subject"))
                product_s_n = clean_value(record.get("Product_S_N"))

                # ✅ Insert into zoho_orders_table
                if outside_agents:
                    cur.execute("""
                        INSERT INTO zoho_orders_table (order_id, so_number, merchant_number, tech_setup_order_options, communication_type, wireless_carrier, terminal_detail, terminal_id, outside_agents, status, est_equip_due_date, equipment_received_date, tracking_number, tracking_number2, purchase_settled, date_shipped, location, subject, product_s_n)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (so_number) DO UPDATE SET
                            merchant_number = EXCLUDED.merchant_number,
                            tech_setup_order_options = EXCLUDED.tech_setup_order_options,
                            communication_type = EXCLUDED.communication_type,
                            wireless_carrier = EXCLUDED.wireless_carrier,
                            terminal_detail = EXCLUDED.terminal_detail,
                            terminal_id = EXCLUDED.terminal_id,
                            outside_agents = EXCLUDED.outside_agents,
                            status = EXCLUDED.status,
                            est_equip_due_date = EXCLUDED.est_equip_due_date,
                            equipment_received_date = EXCLUDED.equipment_received_date,
                            tracking_number = EXCLUDED.tracking_number,
                            tracking_number2 = EXCLUDED.tracking_number2,
                            purchase_settled = EXCLUDED.purchase_settled,
                            date_shipped = EXCLUDED.date_shipped,
                            location = EXCLUDED.location,
                            subject = EXCLUDED.subject,
                            product_s_n = EXCLUDED.product_s_n;
                    """, (order_id, so_number, merchant_number, tech_setup_order_options, communication_type, wireless_carrier, terminal_detail, terminal_id, outside_agents, status, est_equip_due_date, equipment_received_date, tracking_number, tracking_number2, purchase_settled, date_shipped, location, subject, product_s_n))

            conn.commit()
            print(f"📢 {module} - Page {page}: Inserted {len(records)} records. Total so far: {total_records}")

            page += 1
        else:
            print("✅ All data has been retrieved successfully. No more records to fetch.")
            break

    print(f"✅ Total {module} records inserted: {total_records}")

def fetch_products_data(conn, cur, headers):
    """Fetch and insert data for the Products module with unique product_code."""
    API_URL = API_ENDPOINTS["Products"]
    page = 1
    total_records = 0
    module = "Products"

    print(f"\n📢 Fetching data from {module}...\n")

    # Retrieve the last used product_code to continue counting
    cur.execute("SELECT MAX(product_id) FROM zoho_products_table;")
    last_product_id = cur.fetchone()[0] or 0
    last_product_id = last_product_id if last_product_id else 0  # Start from 0 if empty

    while True:
        response = requests.get(f"{API_URL}?page={page}&per_page=200", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            total_records += len(records)

            if not records:
                break  # No more records, stop fetching

            for record in records:
                last_product_id += 1  # Increment product_code counter
                product_id = last_product_id  # Assign new unique ID

                product_code = clean_value(record.get("Product_Code"))
                merchant_number = clean_value(record.get("Merchant_Number"))
                location = clean_value(record.get("Location"))
                assigned = clean_value(record.get("Assigned"))
                product_name = clean_value(record.get("Product_Name"))

                # ✅ Insert into zoho_orders_table
                if product_code:
                    cur.execute("""
                            INSERT INTO zoho_products_table (product_id, product_code, merchant_number, location, assigned, product_name)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (product_code) DO UPDATE SET
                                merchant_number = EXCLUDED.merchant_number,
                                location = EXCLUDED.location,
                                assigned = EXCLUDED.assigned,
                                product_name = EXCLUDED.product_name;
                        """, (product_id, product_code, merchant_number, location, assigned, product_name))
                
                # ✅ Insert into zoho_orders_table
                if location == "Merchant Location":
                    cur.execute("""
                        INSERT INTO products_at_merchants_table (product_id, product_code, merchant_number, location, assigned, product_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_code) DO UPDATE SET
                            merchant_number = EXCLUDED.merchant_number,
                            location = EXCLUDED.location,
                            assigned = EXCLUDED.assigned,
                            product_name = EXCLUDED.product_name;
                    """, (product_id, product_code, merchant_number, location, assigned, product_name))

            conn.commit()
            print(f"📢 {module} - Page {page}: Inserted {len(records)} records. Total so far: {total_records}")

            page += 1
        else:
            print("✅ All data has been retrieved successfully. No more records to fetch.")
            break

    print(f"✅ Total {module} records inserted: {total_records}")

def generate_equipment_report(conn, cur):
    print("\n📢 Generating Equipment Report from zoho_orders_table...\n")

    cur.execute("""
        INSERT INTO equipment_report_table (
            order_id, so_number, merchant_number, tech_setup_order_options, communication_type,
            wireless_carrier, terminal_detail, terminal_id, outside_agents, status,
            est_equip_due_date, equipment_received_date, tracking_number, tracking_number2,
            purchase_settled, date_shipped, location, subject, product_s_n,
            terminal_count, gateway_count, valor_count,
            mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal,
            mpa_valor_virtual_terminal, mpa_valor_ecommerce
        )
        SELECT
            o.order_id, o.so_number, o.merchant_number, o.tech_setup_order_options, o.communication_type,
            o.wireless_carrier, o.terminal_detail, o.terminal_id, o.outside_agents, o.status,
            o.est_equip_due_date, o.equipment_received_date, o.tracking_number, o.tracking_number2,
            o.purchase_settled, o.date_shipped, o.location, o.subject, o.product_s_n,
            1, 1, 1,  -- Placeholder counts (update with real logic later)
            m.mpa_wireless_fee::NUMERIC, m.mpa_valor_portal_access::NUMERIC, m.mpa_valor_add_on_terminal::NUMERIC,
            m.mpa_valor_virtual_terminal::NUMERIC, m.mpa_valor_ecommerce::NUMERIC
        FROM zoho_orders_table o
        JOIN merchants m ON TRIM(LOWER(o.merchant_number)) = TRIM(LOWER(m.merchant_number));
    """)
    conn.commit()
    print("✅ equipment_report_table generated.")


def generate_equipment_pivot(conn, cur):
    print("\n📢 Generating Equipment Pivot Table...\n")

    cur.execute("""
        INSERT INTO equipment_pivot_table (
            merchant_number, outside_agents, terminal_count, gateway_count, valor_count,
            terminal_fee, gateway_fee, valor_fee, equipments_fee,
            mpa_wireless_fee, mpa_valor_portal_access, mpa_valor_add_on_terminal,
            mpa_valor_virtual_terminal, mpa_valor_ecommerce,
            merchant_wireless_terminal_fee, merchant_valor_fee, merchant_second_valor_fee,
            merchant_gateway_fee, total_merchant_share, agent_share
        )
        SELECT
            merchant_number,
            outside_agents,
            SUM(terminal_count),
            SUM(gateway_count),
            SUM(valor_count),
            SUM(terminal_count) * 20,  -- sample rate
            SUM(gateway_count) * 15,
            SUM(valor_count) * 25,
            SUM(terminal_count * 20 + gateway_count * 15 + valor_count * 25),
            AVG(mpa_wireless_fee::NUMERIC),
            AVG(mpa_valor_portal_access::NUMERIC),
            AVG(mpa_valor_add_on_terminal::NUMERIC),
            AVG(mpa_valor_virtual_terminal::NUMERIC),
            AVG(mpa_valor_ecommerce::NUMERIC),
            SUM(terminal_count) * 10,
            SUM(valor_count) * 10,
            SUM(valor_count) * 5,
            SUM(gateway_count) * 7.5,
            SUM(terminal_count * 10 + valor_count * 10 + gateway_count * 7.5),
            SUM(terminal_count * 10 + valor_count * 10 + gateway_count * 7.5) * 0.5
        FROM equipment_report_table
        GROUP BY merchant_number, outside_agents
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    print("✅ equipment_pivot_table generated.")


def generate_equipment_agent_charges(conn, cur):
    print("\n📢 Generating Equipment Agent Charges Table...\n")

    cur.execute("""
        INSERT INTO equipment_agent_charges_table (agent, total_agent_share)
        SELECT outside_agents, SUM(agent_share)
        FROM equipment_pivot_table
        GROUP BY outside_agents
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    print("✅ equipment_agent_charges_table generated.")


def generate_equipment_agent_summary(conn, cur):
    print("\n📢 Generating Equipment Agent Summary Table...\n")

    cur.execute("""
        INSERT INTO equipment_agent_summary_table (outside_agent, total_agent_share)
        SELECT outside_agents, SUM(agent_share)
        FROM equipment_pivot_table
        GROUP BY outside_agents
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    print("✅ equipment_agent_summary_table generated.")





def fetch_and_store_data():
    """Main function to fetch data for Accounts and Sales_Orders separately."""
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
    
    fetch_accounts_data(conn, cur, headers)
    fetch_products_data(conn, cur, headers)
    fetch_orders_data(conn, cur, headers)


    # ✅ Step 2: Generate reports based on stored data
    generate_and_insert_pci_report(conn, cur)
    generate_equipment_report(conn, cur)
    generate_equipment_pivot(conn, cur)
    generate_equipment_agent_charges(conn, cur)
    generate_equipment_agent_summary(conn, cur)

    cur.close()
    conn.close()
    print("✅ All data successfully fetched and stored!")

if __name__ == "__main__":
    fetch_and_store_data()
