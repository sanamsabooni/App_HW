import sys
import os
import pandas as pd
from utils.db_utils import get_db_connection

# Ensure Python looks in the correct directory first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "utils")))

# Get database engine instead of raw connection
engine = get_db_connection()
if engine is None:
    print("❌ Database connection failed. Exiting.")
    sys.exit(1)

# SQL queries
queries = {
    "count_tables": """
        SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table
        UNION ALL
        SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM Agents
        UNION ALL
        SELECT 'merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
    """,
    "full_data": """
        SELECT * FROM zoho_accounts_table;
    """,
    "pci_report": """
        SELECT 
            CAST(m.merchant_number AS TEXT) AS merchant_number,  -- Ensure text format
            m.account_name, 
            m.sales_id, 
            COALESCE(a.partner_name, 'Unknown') AS agent_name,  -- Ensure Agent Name Appears
            TO_CHAR(m.date_approved, 'YYYY-MM') AS approval_month, -- Format approval date
            TO_CHAR(m.date_approved + INTERVAL '2 months', 'Month') AS effective_month, -- Show only month name
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
        WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$';
    """
}

# Execute queries and save results
for name, query in queries.items():
    try:
        df = pd.read_sql_query(query, engine)  # Use engine instead of conn
        if 'merchant_number' in df.columns:
            df['merchant_number'] = df['merchant_number'].astype(str)  # Ensure text format in CSV
        df.to_csv(f"{name}.csv", index=False)
        print(f"✅ Report {name}.csv generated.")
    except Exception as e:
        print(f"❌ Error executing {name}: {e}")

print("✅ Report generation completed.")
