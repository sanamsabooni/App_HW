import streamlit as st
import pandas as pd
from utils.db_utils import get_all_contacts
from zoho_api import get_contacts  

#st.title("Zoho CRM Contacts 📋")
#################################################
# Set page config with the logo as favicon
st.set_page_config(
    page_title="Zoho CRM Contacts",
    page_icon="logo.png",
    layout="wide"
)

# Create a tighter layout for logo and title
col1, col2 = st.columns([0.06, 0.93])  # Reduce left column size to bring elements closer

with col1:
    st.image("logo.png", width=80)  # Logo size remains perfect

with col2:
    st.markdown(
        "<h1 style='margin-top: -5px; margin-left: -15px;'>Zoho Accounts Data Viewer</h1>", 
        unsafe_allow_html=True
    )

#################################################

if st.button("🔄 Fetch and Show Agents"):
    st.write("⏳ Fetching contacts from Zoho CRM...")
    contacts = get_contacts()
    if contacts:
        st.success(f"✅ {len(contacts)} contacts fetched and saved!")
    else:
        st.error("❌ No contacts found or API failed.")

contacts = get_all_contacts()
if contacts.empty:
    st.warning("No data found in the database.")
else:
    per_page = 20  
    total_pages = (len(contacts) // per_page) + (1 if len(contacts) % per_page > 0 else 0)

    page_number = st.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)
    start_idx = (page_number - 1) * per_page
    end_idx = start_idx + per_page

    df_paginated = contacts.iloc[start_idx:end_idx]
    st.dataframe(df_paginated.set_index("S.No"))

    st.write(f"📄 Showing page {page_number} of {total_pages}")
