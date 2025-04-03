# main.py

import logging
from fetch_data import fetch_data_from_zoho
from database import Database
from database import recreate_tables
from fetch_data import fetch_and_store_data
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """Main function to fetch data from Zoho and store it in the database."""
    db = Database()
    
    module_name = "Leads"  # Change this to fetch different module data
    data = fetch_data_from_zoho(module_name)
    
    if data and "data" in data:
        for record in data["data"]:
            query = """
            INSERT INTO leads (id, name, email, phone) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE 
            SET name = EXCLUDED.name, email = EXCLUDED.email, phone = EXCLUDED.phone;
            """
            params = (record.get("id"), record.get("Full_Name"), record.get("Email"), record.get("Phone"))
            db.execute_query(query, params)
        logging.info("Data successfully inserted/updated in the database.")
    else:
        logging.warning("No data received from Zoho.")
    
    db.close_connection()

if __name__ == "__main__":
    main()