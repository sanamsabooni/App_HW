import psycopg2
import pandas as pd
from utils.db_utils import get_db_connection  # Import existing DB connection function
import sys
sys.path.append("utils")  # Ensure Python can find utils module
from db_utils import get_db_connection

# Establish connection
conn = get_db_connection()

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
        SELECT * FROM Agents;
        SELECT * FROM merchants;
    """,
    "merchant_data": """
        SELECT 
            m.id, 
            m.merchant_number, 
            m.account_name, 
            m.sales_id, 
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
    """,
    "pivot_agents": """
        SELECT 
            a.account_name AS agent_name,  
            sub.sales_id, 
            SUM(sub.pci_amnt - sub.pci_fee) AS cumulative_pci_difference
        FROM (
            SELECT 
                m.sales_id, 
                COALESCE(m.pci_amnt::NUMERIC, 0) AS pci_amnt,  
                COALESCE(a.pci_fee::NUMERIC, 0) AS pci_fee
            FROM Merchants m
            LEFT JOIN Agents a 
                ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) 
                OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
            WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
        ) AS sub
        LEFT JOIN Agents a ON TRIM(LOWER(sub.sales_id)) = TRIM(LOWER(a.office_code)) 
                          OR TRIM(LOWER(sub.sales_id)) = TRIM(LOWER(a.office_code_2))
        GROUP BY a.account_name, sub.sales_id
        ORDER BY sub.sales_id;
    """,
    "agent_payments": """
        SELECT 
            a.account_name AS agent_name,  
            SUM(sub.pci_amnt - sub.pci_fee) AS total_pci_difference,
            ROUND(
                CASE 
                    WHEN SUM(sub.pci_amnt - sub.pci_fee) < 0 THEN SUM(sub.pci_amnt - sub.pci_fee)
                    ELSE SUM(sub.pci_amnt - sub.pci_fee) * AVG(sub.split_value)
                END, 2
            ) AS adjusted_pci_value
        FROM (
            SELECT 
                m.sales_id, 
                COALESCE(m.pci_amnt::NUMERIC, 0) AS pci_amnt,  
                COALESCE(a.pci_fee::NUMERIC, 0) AS pci_fee,
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
                ) AS split_value
            FROM Merchants m
            LEFT JOIN Agents a 
                ON TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code)) 
                OR TRIM(LOWER(m.sales_id)) = TRIM(LOWER(a.office_code_2))
            WHERE m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
        ) AS sub
        LEFT JOIN Agents a 
            ON TRIM(LOWER(sub.sales_id)) = TRIM(LOWER(a.office_code)) 
            OR TRIM(LOWER(sub.sales_id)) = TRIM(LOWER(a.office_code_2))
        GROUP BY a.account_name
        ORDER BY total_pci_difference DESC;
    """
}

# Execute queries and save results
for name, query in queries.items():
    df = pd.read_sql_query(query, conn)
    df.to_csv(f"{name}.csv", index=False)  # Save each query result as CSV
    print(f"Report {name}.csv generated.")

# Close connection
conn.close()
