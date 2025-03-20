import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from database import get_db_connection


# Database Connection
engine = get_db_connection()

# Function to Fetch Product Location Data
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
            df = pd.DataFrame(rows, columns=["location", "count"])
        return df
    finally:
        conn.close()

def autopct_format(pct):
    return f'{pct:.1f}%' if pct >= 6 else ''  # Hide percentages less than 6%


# Function to Show Visualization
def show_visualization():
    df = fetch_product_locations()
    if df.empty:
        st.write("No location data available.")
        return None
    
    total_count = df['count'].sum()
    colors = plt.cm.Paired.colors[:len(df)]
    
    col1, col2 = st.columns([0.4, 0.6], gap="large")
    
    with col1:
        st.subheader("Color Mapping with Percentages")
        mapping_table = """
        <table style='width:100%; border-collapse: collapse; text-align: left;'>
        <tr>
            <th style='padding: 8px; border-bottom: 1px solid black;'>Color</th>
            <th style='padding: 8px; border-bottom: 1px solid black;'>Location</th>
            <th style='padding: 8px; border-bottom: 1px solid black;'>Percentage</th>
        </tr>
        """
        
        for label, count, color in zip(df['location'], df['count'], colors):
            percentage = (count / total_count) * 100
            mapping_table += (
                f"<tr>"
                f"<td style='padding: 8px;'><svg width='21' height='21'><circle cx='10' cy='10' r='9' fill='rgb({int(color[0]*255)}, {int(color[1]*255)}, {int(color[2]*255)})' /></svg></td>"
                f"<td style='padding: 8px; color: black;'>{label}</td>"
                f"<td style='padding: 8px; color: black;'>{percentage:.1f}%</td>"
                f"</tr>"
            )
        
        mapping_table += "</table>"
        st.markdown(mapping_table, unsafe_allow_html=True)
        
    with col2:
        st.subheader("Pie Chart - Product Locations")
        fig, ax = plt.subplots()
        ax.pie(df['count'], startangle=90, colors=colors, autopct=lambda pct: autopct_format(pct))
        ax.axis("equal")
        st.pyplot(fig)
    
    return df