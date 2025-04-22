import streamlit as st
import psycopg2
import os
import re
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

# Attempt to import reports from streamlit_app
try:
    from streamlit_app import pci_report, equipment_pivot_report
except ImportError:
    pci_report = pd.DataFrame()
    equipment_pivot_report = pd.DataFrame()

def find_merchant_number(text):
    match = re.search(r'\d{6,}', text)
    return match.group(0) if match else None

def classify_question(text):
    text = text.lower()
    if "equipment" in text:
        return "equipment_fee"
    elif "pci" in text:
        return "pci_fee"
    elif "commission" in text:
        return "commission"
    elif "orders" in text or "shipped" in text:
        return "orders"
    elif "unpaid merchants" in text or ("unpaid" in text and "merchants" in text):
        return "unpaid_merchants"
    elif "clawback" in text:
        return "clawbacks"
    elif "agents" in text and ("split over" in text or "split greater" in text):
        return "high_split_agents"
    else:
        return "unknown"

def run_query(sql, params=None):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        result = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(result, columns=colnames)
        cur.close()
        conn.close()
        return df
    except Exception as e:
        return str(e)

# 🔁 Make this callable from streamlit_app
def run_chatbot():
    st.title("🧠 RDS Chatbot (Free Version)")
    question = st.text_input("Ask a question about merchants, orders, agents, etc.")

    if question:
        merchant_number = find_merchant_number(question)
        intent = classify_question(question)

        # General Queries
        if intent == "unpaid_merchants":
            sql = "SELECT merchant_number, account_name, paid FROM merchants_table WHERE paid = false"
            result = run_query(sql)

        elif intent == "clawbacks":
            sql = "SELECT merchant_number, account_name, clawback, clawback_date FROM merchants_table WHERE clawback IS NOT NULL"
            result = run_query(sql)

        elif intent == "high_split_agents":
            sql = "SELECT partner_name, account_name, split FROM agents_table WHERE split::float > 0.8"
            result = run_query(sql)

        elif intent == "orders" and not merchant_number:
            sql = """
                SELECT so_number, merchant_number, date_shipped, status
                FROM zoho_orders_table
                WHERE date_shipped >= CURRENT_DATE - INTERVAL '30 days'
            """
            result = run_query(sql)

        # Merchant-Specific Queries
        elif merchant_number:
            if intent == "equipment_fee" and not equipment_pivot_report.empty:
                match = equipment_pivot_report[equipment_pivot_report['merchant_number'].astype(str) == merchant_number]
                result = match[['merchant_number', 'Total Merchant Share', 'Equipments Fee']].copy() if not match.empty else pd.DataFrame()

            elif intent == "pci_fee" and not pci_report.empty:
                match = pci_report[pci_report['merchant_number'].astype(str) == merchant_number]
                result = match[['merchant_number', 'pci_amnt', 'pci_share']].copy() if not match.empty else pd.DataFrame()
                if not result.empty:
                    result.rename(columns={'pci_amnt': 'PCI Fee', 'pci_share': 'Merchant Share'}, inplace=True)

            elif intent == "commission":
                sql = """
                    SELECT account_name, commission_amount, commission_pay_date
                    FROM merchants_table
                    WHERE merchant_number = %s
                """
                result = run_query(sql, (merchant_number,))

            elif intent == "orders":
                sql = """
                    SELECT so_number, date_shipped, status
                    FROM zoho_orders_table
                    WHERE merchant_number = %s
                """
                result = run_query(sql, (merchant_number,))

            else:
                sql = "SELECT * FROM merchants_table WHERE merchant_number = %s"
                result = run_query(sql, (merchant_number,))

        else:
            st.warning("Please include a merchant number or try a general report question.")
            result = []

        # Output Results
        if isinstance(result, str):
            st.error(f"Database error: {result}")
        elif isinstance(result, pd.DataFrame) and result.empty:
            st.warning("No results found.")
        elif isinstance(result, pd.DataFrame):
            result.rename(columns={
                "mpa_wireless_fee": "Equipment Fee",
                "mpa_valor_portal_access": "Portal Access Fee",
                "pci_fee": "PCI Fee"
            }, inplace=True)
            st.success(f"✅ Found {len(result)} record(s)")
            st.dataframe(result, use_container_width=True)
