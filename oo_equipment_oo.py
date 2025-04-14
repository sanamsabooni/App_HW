import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection

class EquipmentReport:
    def __init__(self):
        self.conn = get_db_connection()

    def load_main_report(self):
        """Load the main equipment report."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = "SELECT * FROM equipment_report_table ORDER BY order_id DESC;"
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading equipment report: {e}")
            return pd.DataFrame()

    def load_pivot_report(self):
        """Load the equipment pivot report by merchant."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = "SELECT * FROM equipment_pivot_table ORDER BY valor_count DESC;"
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading pivot report: {e}")
            return pd.DataFrame()

    def load_agent_charges(self):
        """Load the agent charges for equipment."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = "SELECT * FROM equipment_agent_charges_table ORDER BY total_agent_share DESC;"
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading agent charges: {e}")
            return pd.DataFrame()

    def load_agent_summary(self):
        """Load the agent summary for equipment."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = "SELECT * FROM equipment_agent_summary_table ORDER BY total_agent_share DESC;"
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            st.error(f"❌ Error loading agent summary: {e}")
            return pd.DataFrame()

    def render_main_report(self):
        st.subheader("🔧 Equipment Report")
        df = self.load_main_report()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No Equipment report data available.")

    def render_pivot_report(self):
        st.subheader("📊 Equipment Pivot Report (Merchant View)")
        df = self.load_pivot_report()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No pivot report data available.")

    def render_agent_charges(self):
        st.subheader("💼 Equipment Agent Charges")
        df = self.load_agent_charges()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No agent charge data available.")

    def render_agent_summary(self):
        st.subheader("📋 Equipment Agent Summary")
        df = self.load_agent_summary()
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No agent summary data available.")
