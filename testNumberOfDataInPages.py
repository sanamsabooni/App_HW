'''
This script tests the pagination functionality when fetching data.
It ensures that the number of records retrieved per page matches the expected count.
'''

import unittest
from fetch_data import fetch_data_from_zoho

class TestPagination(unittest.TestCase):
    """Unit test for verifying the number of records fetched per page."""
    
    def test_data_pagination(self):
        """Fetch data and verify pagination limits."""
        module_name = "Leads"
        data = fetch_data_from_zoho(module_name)
        
        if data and "data" in data:
            self.assertLessEqual(len(data["data"]), 200, "Number of records per page exceeds 200!")
        else:
            self.fail("No data returned from Zoho API")

if __name__ == "__main__":
    unittest.main()
