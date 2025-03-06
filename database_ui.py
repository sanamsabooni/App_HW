import tkinter as tk
from tkinter import ttk, messagebox
import psycopg2
import pandas as pd
import ace_tools as tools
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings from .env
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "hubwallet_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432")
}

def execute_query(query):
    """Executes a given SQL query and returns results as a DataFrame."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        messagebox.showerror("Database Error", str(e))
        return None

def show_table_data(table_name):
    """Displays the data from the selected table."""
    query = f"SELECT * FROM {table_name};"
    df = execute_query(query)
    if df is not None:
        tools.display_dataframe_to_user(name=f"{table_name} Data", dataframe=df)

def show_instance_counts():
    """Shows the count of instances for each table."""
    query = """
    SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table
    UNION ALL
    SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM Agents
    UNION ALL
    SELECT 'merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
    """
    df = execute_query(query)
    if df is not None:
        tools.display_dataframe_to_user(name="Table Instances", dataframe=df)

def show_merchant_report():
    """Displays Merchant Report 1."""
    query = """
    SELECT 
        m.id, m.merchant_number, m.account_name, m.sales_id, 
        a.account_name AS agent_name, 
        TO_CHAR(m.date_approved, 'Month') AS approved_month, 
        TO_CHAR(m.date_approved + INTERVAL '2 months', 'Month') AS pci_month, 
        COALESCE(a.pci_fee::NUMERIC, 0) AS pci_fee,  
        COALESCE(m.pci_amnt::NUMERIC, 0) AS pci_amnt,  
        ROUND(
            COALESCE(
                CASE 
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) THEN 
                        (REGEXP_REPLACE(a.split, '[^0-9]', '', 'g')::NUMERIC / 100)
                    WHEN TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2)) THEN 
                        (REGEXP_REPLACE(a.split_2, '[^0-9]', '', 'g')::NUMERIC / 100)
                    ELSE 0
                END, 0
            ), 2
        ) AS split_value, 
        COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0) AS pci_difference
    FROM Merchants m
    LEFT JOIN Agents a 
        ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) 
        OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
    WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
    ORDER BY pci_month, m.sales_id;
    """
    df = execute_query(query)
    if df is not None:
        tools.display_dataframe_to_user(name="Merchant Report", dataframe=df)

def show_agent_report():
    """Displays Agent Report 1."""
    query = """
    SELECT 
        TO_CHAR(m.date_approved + INTERVAL '2 months', 'Month') AS pci_month, 
        a.account_name AS agent_name, 
        SUM(COALESCE(m.pci_amnt::NUMERIC, 0) - COALESCE(a.pci_fee::NUMERIC, 0)) AS cumulative_pci_difference
    FROM Merchants m
    LEFT JOIN Agents a 
        ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) 
        OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
    WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
    GROUP BY pci_month, agent_name
    ORDER BY pci_month, agent_name;
    """
    df = execute_query(query)
    if df is not None:
        tools.display_dataframe_to_user(name="Agent Report", dataframe=df)

# Create UI window
root = tk.Tk()
root.title("Hubwallet Database UI")
root.geometry("400x400")

# Buttons for actions
btn_instance_counts = ttk.Button(root, text="Show Table Instance Counts", command=show_instance_counts)
btn_instance_counts.pack(pady=5)

btn_zoho_accounts = ttk.Button(root, text="Show Zoho Accounts Table", command=lambda: show_table_data("zoho_accounts_table"))
btn_zoho_accounts.pack(pady=5)

btn_agents = ttk.Button(root, text="Show Agents Table", command=lambda: show_table_data("Agents"))
btn_agents.pack(pady=5)

btn_merchants = ttk.Button(root, text="Show Merchants Table", command=lambda: show_table_data("merchants"))
btn_merchants.pack(pady=5)

btn_merchant_report = ttk.Button(root, text="Show Merchant Report", command=show_merchant_report)
btn_merchant_report.pack(pady=5)

btn_agent_report = ttk.Button(root, text="Show Agent Report", command=show_agent_report)
btn_agent_report.pack(pady=5)

# Run the UI
tk.mainloop()