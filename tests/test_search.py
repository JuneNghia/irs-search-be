import unittest
from app.search_service import search_query

class TestSearchQuery(unittest.TestCase):
    def test_search(self):
        result = search_query("test query")
        self.assertIsInstance(result, dict)