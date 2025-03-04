# config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Zoho API Configuration
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_API_BASE_URL = "https://www.zohoapis.com/crm/v2"

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "hubwallet_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Other Constants
TOKEN_REFRESH_INTERVAL = 3600  # In seconds (1 hour)
