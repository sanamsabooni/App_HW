# Hubwallet Zoho API Integration

## Installation
1. Clone the repository

HubwalletProject/
│── .env                        # Environment variables
│── zoho_api.py                 # Fetches Zoho CRM contacts and saves to DB
│── database.py                  # Handles database connection via SSH Tunnel
│── config.py                     # Configuration for API and DB
│── requirements.txt               # Dependencies
│── README.md                      # Documentation
│── utils/
│   ├── __init__.py                # Marks utils as a module
│   ├── zoho_utils.py               # Zoho API authentication and utilities
│   ├── db_utils.py                  # Database helper functions
│── streamlit_app.py                  # Streamlit UI to display contacts
│── logs/
│   ├── app.log                     # Logs for debugging


App_HW-main/
│── fetch_data.py  # NEW (handles fetching & storing)
│── zoho_api.py    # (keeps only authentication logic)
│── database.py
│── utils/
│   ├── db_utils.py
│   ├── zoho_utils.py
│── streamlit_app.py
│── config.py
│── requirements.txt
│── README.md
