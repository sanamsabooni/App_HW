import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

ZOHO_BASE = os.getenv("ZOHO_BASE")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_REDIRECT_URI = os.getenv("ZOHO_REDIRECT_URI")

access_token = None
access_token_expiration = None

def refresh_access_token():
    """Refresh Zoho OAuth access token."""
    global access_token, access_token_expiration

    if access_token and access_token_expiration > time.time():
        return access_token

    url = f"{ZOHO_BASE}/oauth/v2/token"
    data = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "redirect_uri": ZOHO_REDIRECT_URI,
        "grant_type": "refresh_token",
    }

    response = requests.post(url, data=data)

    if response.status_code != 200:
        print(f"❌ Failed to refresh access token: {response.status_code} {response.text}")
        return None

    response_data = response.json()
    access_token = response_data["access_token"]
    access_token_expiration = time.time() + response_data["expires_in"]
    return access_token
