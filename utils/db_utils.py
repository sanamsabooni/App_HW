'''
This module provides utility functions for database operations.
It includes helper functions for executing queries and fetching results.
'''

import logging
from database import Database

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def insert_or_update_lead(record):
    """Insert or update a lead in the database."""
    db = Database()
    query = """
    INSERT INTO leads (id, name, email, phone) 
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE 
    SET name = EXCLUDED.name, email = EXCLUDED.email, phone = EXCLUDED.phone;
    """
    params = (
        record.get("id"),
        record.get("Full_Name"),
        record.get("Email"),
        record.get("Phone")
    )
    
    db.execute_query(query, params)
    db.close_connection()

# Example usage (uncomment to test)
# if __name__ == "__main__":
#     sample_record = {"id": 1, "Full_Name": "John Doe", "Email": "john@example.com", "Phone": "1234567890"}
#     insert_or_update_lead(sample_record)