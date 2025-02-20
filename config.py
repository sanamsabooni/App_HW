import os
from dotenv import load_dotenv

load_dotenv()

# Zoho API Config
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_BASE = os.getenv("ZOHO_BASE")
ZOHO_API_BASE = os.getenv("ZOHO_API_BASE")
ZOHO_REDIRECT_URI = os.getenv("ZOHO_REDIRECT_URI")

# Database Config
DB_CONFIG = {
    "host": os.getenv("RDS_HOST"),
    "port": os.getenv("RDS_PORT"),
    "database": os.getenv("RDS_DB"),
    "user": os.getenv("RDS_USER"),
    "password": os.getenv("RDS_PASSWORD"),
}
