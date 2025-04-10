import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection

class TableStats:
    def __init__(self):
        self.conn = get_db_connection()

    def load_table_counts(self):
        """Load count of records from key tables."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = """
            SELECT 'zoho_accounts_table' AS table_name, COUNT(*) AS row_count FROM zoho_accounts_table
            UNION ALL 
            SELECT 'Sales Orders' AS table_name, COUNT(*) AS row_count FROM zoho_orders_table
            UNION ALL 
            SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM zoho_products_table
            UNION ALL 
            SELECT 'Products' AS table_name, COUNT(*) AS row_count FROM products_at_merchants_table
            UNION ALL 
            SELECT 'Agents' AS table_name, COUNT(*) AS row_count FROM agents 
            UNION ALL 
            SELECT 'Merchants' AS table_name, COUNT(*) AS row_count FROM merchants;
        """

        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading table counts: {e}")
            return pd.DataFrame()

    def load_accounts_full(self):
        """Load full data from zoho_accounts_table."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        try:
            return pd.read_sql_query("SELECT * FROM zoho_accounts_table;", self.conn)
        except Exception as e:
            st.error(f"❌ Error loading accounts full data: {e}")
            return pd.DataFrame()
