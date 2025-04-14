import streamlit as st
import pandas as pd
from utils.db_utils import get_db_connection

class PCIReport:
    def __init__(self):
        self.conn = get_db_connection()

    def load_data(self):
        """Load PCI report data from the database table."""
        if not self.conn:
            st.error("❌ Could not connect to the database.")
            return pd.DataFrame()

        query = "SELECT * FROM pci_report_table ORDER BY approval_month DESC;"
        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            st.error(f"❌ Error loading PCI report: {e}")
            return pd.DataFrame()

    def render(self):
        """Render the PCI report in Streamlit."""
        st.subheader("📄 PCI Report")

        df = self.load_data()
        if df.empty:
            st.info("No PCI report data available.")
            return

        # Optional: clean or format the dataframe here
        if 'pci_difference' in df.columns:
            df = df.drop(columns=['pci_difference'])

        st.dataframe(df)
