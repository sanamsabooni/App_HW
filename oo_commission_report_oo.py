import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection

class CommissionReport:
    def __init__(self):
        self.conn = get_db_connection()

    def load_full_report(self):
        """Load full commission report from zoho_accounts_table."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = """
            SELECT
                account_name,
                processor,
                account_status,
                approved,
                date_approved,
                sales_id,
                outside_agents AS outside_agent,
                commission_amount,
                commission_pay_date,
                paid,
                clawback,
                clawback_date
            FROM zoho_accounts_table
            WHERE sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
              AND processor IS NOT NULL
        """

        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading commission report: {e}")
            return pd.DataFrame()

    def load_closed_pending_clawback(self):
        """Load closed accounts with commission but pending clawback."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = """
            SELECT 
                outside_agents AS outside_agent,
                account_name,
                commission_amount,
                commission_pay_date,
                account_status,
                clawback,
                clawback_date
            FROM zoho_accounts_table
            WHERE 
                commission_pay_date IS NOT NULL
                AND clawback_date IS NULL
                AND account_status = 'Closed';
        """

        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading pending clawback report: {e}")
            return pd.DataFrame()

    def render_full_report(self):
        st.subheader("💸 Commission Report")
        df = self.load_full_report()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No commission report data available.")

    def render_pending_clawback(self):
        st.subheader("⚠️ Closed Accounts with Pending Clawbacks")
        df = self.load_closed_pending_clawback()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No pending clawback records found.")
