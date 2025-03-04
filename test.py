'''
This script contains unit tests for the Database class.
It tests the following functionalities:
1. Establishing a database connection.
2. Inserting data into a test table.
3. Fetching the inserted data to verify correctness.
4. Cleaning up by dropping the test table after execution.
'''

import unittest
from database import Database

class TestDatabase(unittest.TestCase):
    """Unit tests for the Database class."""
    
    def setUp(self):
        """Set up a test database connection."""
        self.db = Database()
    
    def test_connection(self):
        """Test if database connection is established."""
        self.assertIsNotNone(self.db.conn)
        self.assertIsNotNone(self.db.cursor)
    
    def test_insert_and_fetch(self):
        """Test inserting and fetching data from the database."""
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS test (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
        ""
        )
        
        self.db.execute_query("INSERT INTO test (name) VALUES (%s);", ("Sample Name",))
        result = self.db.fetch_data("SELECT name FROM test WHERE name = %s;", ("Sample Name",))
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "Sample Name")
    
    def tearDown(self):
        """Clean up after tests."""
        self.db.execute_query("DROP TABLE IF EXISTS test;")
        self.db.close_connection()

if __name__ == "__main__":
    unittest.main()
