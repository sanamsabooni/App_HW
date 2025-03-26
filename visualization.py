import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from database import get_db_connection

# Database Connection
engine = get_db_connection()


def fetch_inventory_on_site_products():
    query = """
        SELECT LOWER(TRIM(product_name)) AS normalized_name, COUNT(*) as count 
        FROM zoho_products_table 
        WHERE location = 'Inventory On Site' 
        GROUP BY normalized_name 
        ORDER BY count DESC
    """
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=["label", "count"])
            df["label"] = df["label"].str.title()
        return df
    finally:
        conn.close()


        
def fetch_active_agent_table():
    query = """
        SELECT LOWER(TRIM(a.outside_agents)) AS normalized_name, COUNT(DISTINCT a.merchant_number) AS count
        FROM zoho_accounts_table a
        JOIN merchants m ON a.account_name = m.account_name
        WHERE a.merchant_number IS NOT NULL
          AND a.outside_agents IS NOT NULL
          AND m.sales_id ~ '^[A-Za-z]{2}[0-9]{2}$'
        GROUP BY normalized_name
        ORDER BY count DESC
    """
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=["label", "count"])
            df["label"] = df["label"].str.title()
        return df
    finally:
        conn.close()





# Function to Fetch Product Location Data for "Inventory On Site"
def fetch_inventory_on_site_products():
    query = """
        SELECT LOWER(TRIM(product_name)) AS normalized_name, COUNT(*) as count 
        FROM zoho_products_table 
        WHERE location = 'Inventory On Site' 
        GROUP BY normalized_name 
        ORDER BY count DESC
    """
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=["label", "count"])
            df["label"] = df["label"].str.title()  # Format nicely for display
        return df
    finally:
        conn.close()

# Function to Fetch Product Location Summary
def fetch_product_locations():
    query = """
        SELECT location, COUNT(*) as count 
        FROM zoho_products_table 
        WHERE location IS NOT NULL 
        GROUP BY location 
        ORDER BY count DESC
    """
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=["label", "count"])
            df["label"] = df["label"].str.title()
        return df
    finally:
        conn.close()

# Format percentage labels
def autopct_format(pct):
    return f'{pct:.1f}%' if pct >= 3 else ''  # Hide percentages less than 3%


def show_visualization(sub_page=None):
    if sub_page == "Product Locations":
        df = fetch_product_locations()
        context_title = "Location"
    elif sub_page in ["Available Product"]:
        df = fetch_inventory_on_site_products()
        context_title = "Product Name"
    elif sub_page == "Active Agents":
        df = fetch_active_agent_table()
        context_title = "Outside Agent"
    else:
        return None

    if df.empty:
        st.write("No data available for this visualization.")
        return None

    count = df['count'].sum()
    colors = plt.cm.Paired.colors[:len(df)]

    col1, col2 = st.columns([0.4, 0.6], gap="large")

    with col1:
        st.subheader("Color Mapping with Counts")
        mapping_table = f"""
        <table style='width:100%; border-collapse: collapse; text-align: left;'>
        <tr>
            <th style='padding: 8px; border-bottom: 1px solid black;'>Color</th>
            <th style='padding: 8px; border-bottom: 1px solid black;'>{context_title}</th>
            <th style='padding: 8px; border-bottom: 1px solid black;'>Merchant Count</th>
        </tr>
        """

        for label, count, color in zip(df['label'], df['count'], colors):
            mapping_table += (
                f"<tr>"
                f"<td style='padding: 8px;'><svg width='21' height='21'><circle cx='10' cy='10' r='9' fill='rgb({int(color[0]*255)}, {int(color[1]*255)}, {int(color[2]*255)})' /></svg></td>"
                f"<td style='padding: 8px; color: black;'>{label}</td>"
                f"<td style='padding: 8px; color: black;'>{count:.1f}</td>"
                f"</tr>"
            )

        mapping_table += "</table>"
        st.markdown(mapping_table, unsafe_allow_html=True)

    with col2:
        st.subheader("Pie Chart - Distribution")
        fig, ax = plt.subplots()

        counts = df['count']
        labels = df['label']
        total = counts.sum()

        percentages = 100 * counts / total
        display_labels = [label if pct > 2.5 else '' for label, pct in zip(labels, percentages)]

        ax.pie(counts, labels=display_labels, startangle=90, colors=colors, autopct=lambda pct: autopct_format(pct))
        ax.axis("equal")
        st.pyplot(fig)

    # Show Full Table
    st.markdown("---")
    st.subheader(f"Full List with Counts by {context_title}")
    st.dataframe(df.rename(columns={"label": context_title, "count": "Merchant Count"}), use_container_width=True)

    return df