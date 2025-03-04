'''
This script tests whether the API correctly handles fetching more than 200 records.
It verifies that pagination works properly when the dataset exceeds the 200-record limit.
'''

import unittest
from fetch_data import fetch_data_from_zoho

class TestOver200Records(unittest.TestCase):
    """Unit test for fetching more than 200 records from Zoho API."""
    
    def test_fetch_more_than_200(self):
        """Ensure that multiple pages are fetched if data exceeds 200 records."""
        module_name = "Leads"
        all_data = []
        page = 1

        while True:
            data = fetch_data_from_zoho(f"{module_name}?page={page}")
            
            if not data or "data" not in data:
                break
            
            all_data.extend(data["data"])
            
            if len(data["data"]) < 200:
                break  # Stop if the last page contains less than 200 records
            
            page += 1
        
        self.assertGreater(len(all_data), 200, "Total records fetched should exceed 200.")

if __name__ == "__main__":
    unittest.main()
