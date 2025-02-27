import requests
from config import ZOHO_API_URL, ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN

def get_access_token():
    """Fetches a new Zoho OAuth access token."""
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    
    response = requests.post(url, data=payload)
    data = response.json()

    if "access_token" in data:
        return data["access_token"]
    else:
        print(f"❌ Failed to refresh token: {data}")
        return None

def fetch_zoho_records(start_index=1, per_page=200):
    """Fetches paginated records from Zoho API."""
    access_token = get_access_token()
    if not access_token:
        return []
    
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    url = f"{ZOHO_API_URL}?start_index={start_index}&per_page={per_page}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        print(f"❌ API Request Failed: {response.text}")
        return []
