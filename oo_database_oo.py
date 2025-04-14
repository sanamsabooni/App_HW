import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database credentials from environment variables
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establish and return a database connection."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def recreate_tables():
    """Drops tables if they exist and recreates them."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                drop_tables_sql = """
                DROP TABLE IF EXISTS zoho_accounts_table CASCADE;
                DROP TABLE IF EXISTS agents CASCADE;
                DROP TABLE IF EXISTS merchants CASCADE;
                DROP TABLE IF EXISTS zoho_orders_table CASCADE;
                DROP TABLE IF EXISTS zoho_products_table CASCADE;
                DROP TABLE IF EXISTS products_at_merchants_table CASCADE;
                DROP TABLE IF EXISTS pci_report_table CASCADE;
                DROP TABLE IF EXISTS equipment_report_table CASCADE;
                DROP TABLE IF EXISTS equipment_pivot_table CASCADE;
                DROP TABLE IF EXISTS equipment_agent_charges_table CASCADE;
                DROP TABLE IF EXISTS equipment_agent_summary_table CASCADE;
                """

                create_zoho_accounts_table_sql = """
                CREATE TABLE zoho_accounts_table (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    partner_name TEXT,
                    office_code TEXT,
                    office_code_2 TEXT,
                    split TEXT,
                    split_2 TEXT,
                    pci_fee TEXT,
                    merchant_number TEXT UNIQUE,
                    sales_id TEXT,
                    outside_agents TEXT,
                    pci_amnt TEXT,
                    account_name TEXT,
                    date_approved DATE,
                    account_status TEXT,
                    mpa_wireless_fee TEXT,
                    mpa_valor_portal_access TEXT,
                    mpa_Valor_add_on_terminal TEXT,
                    mpa_valor_virtual_terminal TEXT,
                    mpa_valor_ecommerce TEXT,
                    processor TEXT,
                    approved TEXT,
                    commission_amount TEXT,
                    commission_pay_date TEXT,
                    paid BOOLEAN,
                    clawback TEXT,
                    clawback_date DATE
                );
                """

                create_pci_report_table_sql = """
                CREATE TABLE pci_report_table (
                    merchant_number TEXT,
                    account_name TEXT,
                    sales_id TEXT,
                    agent_name TEXT,
                    approval_month TEXT,
                    effective_month TEXT,
                    pci_fee NUMERIC,
                    pci_amnt NUMERIC,
                    split_value NUMERIC,
                    pci_share NUMERIC,
                    max_share NUMERIC
                );

                """

                create_equipment_report_table_sql = """
                CREATE TABLE equipment_report_table (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER,
                    so_number TEXT,
                    merchant_number TEXT,
                    tech_setup_order_options TEXT,
                    communication_type TEXT,
                    wireless_carrier TEXT,
                    terminal_detail TEXT,
                    terminal_id TEXT,
                    outside_agents TEXT,
                    status TEXT,
                    est_equip_due_date DATE,
                    equipment_received_date DATE,
                    tracking_number TEXT,
                    tracking_number2 TEXT,
                    purchase_settled DATE,
                    date_shipped DATE,
                    location TEXT,
                    subject TEXT,
                    product_s_n TEXT,
                    terminal_count INTEGER,
                    gateway_count INTEGER,
                    valor_count INTEGER,
                    mpa_wireless_fee TEXT,
                    mpa_valor_portal_access TEXT,
                    mpa_valor_add_on_terminal TEXT,
                    mpa_valor_virtual_terminal TEXT,
                    mpa_valor_ecommerce TEXT
                );

                """

                create_equipment_pivot_table_sql = """
                CREATE TABLE equipment_pivot_table (
                    id SERIAL PRIMARY KEY,
                    merchant_number TEXT,
                    outside_agents TEXT,
                    terminal_count INTEGER,
                    gateway_count INTEGER,
                    valor_count INTEGER,
                    terminal_fee NUMERIC,
                    gateway_fee NUMERIC,
                    valor_fee NUMERIC,
                    equipments_fee NUMERIC,
                    mpa_wireless_fee NUMERIC,
                    mpa_valor_portal_access NUMERIC,
                    mpa_valor_add_on_terminal NUMERIC,
                    mpa_valor_virtual_terminal NUMERIC,
                    mpa_valor_ecommerce NUMERIC,
                    merchant_wireless_terminal_fee NUMERIC,
                    merchant_valor_fee NUMERIC,
                    merchant_second_valor_fee NUMERIC,
                    merchant_gateway_fee NUMERIC,
                    total_merchant_share NUMERIC,
                    agent_share NUMERIC
                );
                """

                create_equipment_agent_charges_table_sql = """
                CREATE TABLE equipment_agent_charges_table (
                    id SERIAL PRIMARY KEY,
                    agent TEXT,
                    total_agent_share NUMERIC
                );
                """

                create_equipment_agent_summary_table_sql = """
                CREATE TABLE equipment_agent_summary_table (
                    id SERIAL PRIMARY KEY,
                    outside_agent TEXT,
                    total_agent_share NUMERIC
                );
                """

                create_agents_table_sql = """
                CREATE TABLE agents (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    partner_name TEXT,
                    office_code TEXT,
                    office_code_2 TEXT,
                    split TEXT,
                    split_2 TEXT,
                    pci_fee TEXT,
                    account_name TEXT UNIQUE
                );
                """

                create_merchants_table_sql = """
                CREATE TABLE merchants (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT UNIQUE NOT NULL,
                    merchant_number TEXT UNIQUE NOT NULL,
                    account_name TEXT,
                    sales_id TEXT,
                    outside_agents TEXT,
                    pci_amnt TEXT,
                    date_approved DATE,
                    account_status TEXT,
                    mpa_wireless_fee TEXT,
                    mpa_valor_portal_access TEXT,
                    mpa_Valor_add_on_terminal TEXT,
                    mpa_valor_virtual_terminal TEXT,
                    mpa_valor_ecommerce TEXT,
                    processor TEXT,
                    approved TEXT,
                    commission_amount TEXT,
                    commission_pay_date TEXT,
                    paid BOOLEAN,
                    clawback TEXT,
                    clawback_date DATE
                );
                """

                create_zoho_orders_table_sql = """
                CREATE TABLE zoho_orders_table (
                    order_id  SERIAL PRIMARY KEY,
                    so_number TEXT UNIQUE,
                    merchant_number TEXT,
                    tech_setup_order_options TEXT,
                    communication_type TEXT, 
                    wireless_carrier TEXT,
                    terminal_detail TEXT,
                    terminal_id TEXT,
                    outside_agents TEXT,
                    status TEXT, 
                    est_equip_due_date Date,
                    equipment_received_date DATE,
                    tracking_number TEXT,
                    tracking_number2 TEXT,
                    purchase_settled DATE,
                    date_shipped DATE,
                    location TEXT,
                    subject TEXT,
                    product_s_n TEXT
                );
                """

                create_zoho_products_table_sql = """
                CREATE TABLE zoho_products_table (
                    product_id SERIAL PRIMARY KEY,
                    product_code TEXT UNIQUE NOT NULL,
                    merchant_number TEXT,
                    location TEXT,
                    assigned BOOLEAN,
                    product_name TEXT
                );
                """

                create_products_at_merchants_table_sql = """
                CREATE TABLE products_at_merchants_table (
                    product_id SERIAL PRIMARY KEY,
                    product_code TEXT UNIQUE NOT NULL,
                    merchant_number TEXT,
                    location TEXT,
                    assigned BOOLEAN,
                    product_name TEXT
                );
                """

                # Execute SQL statements
                cur.execute(drop_tables_sql)
                cur.execute(create_zoho_accounts_table_sql)
                cur.execute(create_pci_report_table_sql)
                cur.execute(create_equipment_report_table_sql)
                cur.execute(create_equipment_pivot_table_sql)
                cur.execute(create_equipment_agent_charges_table_sql)
                cur.execute(create_equipment_agent_summary_table_sql)
                cur.execute(create_agents_table_sql)
                cur.execute(create_merchants_table_sql)
                cur.execute(create_zoho_orders_table_sql)
                cur.execute(create_zoho_products_table_sql)
                cur.execute(create_products_at_merchants_table_sql)

                print("✅ Tables dropped and recreated successfully.")

    except psycopg2.Error as e:
        print(f"❌ Error while recreating tables: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    recreate_tables()
