'''
This module provides utility functions for interacting with the Zoho API.
It includes functions for fetching data and handling API responses.
'''

import requests
import logging
from zoho_api import refresh_access_token
from config import ZOHO_API_BASE_URL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_zoho_module_data(module_name):
    """Fetch data from a specific Zoho module."""
    access_token = refresh_access_token()
    if not access_token:
        logging.error("Failed to retrieve access token. Cannot fetch data.")
        return None
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    url = f"{ZOHO_API_BASE_URL}/{module_name}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from Zoho module: {module_name}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Zoho API: {e}")
        return None

# Example usage (uncomment to test)
# if __name__ == "__main__":
#     data = fetch_zoho_module_data("Leads")
#     print(data)
