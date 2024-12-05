import unittest
from unittest.mock import patch
from backend.lambda_functions.data_ingestor.lambda_function import fetch_maritime_data, process_open_sky_data

class TestDataIngestor(unittest.TestCase):
    @patch('data_ingestor.lambda_function.requests.get')
    def test_fetch_maritime_data(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = {'states': [...]}
        
        result = fetch_maritime_data()
        self.assertEqual(result['status'], 'success')
        # Add more assertions to verify the expected behavior
    
    def test_process_open_sky_data(self):
        data = {'states': [...]}
        result = process_open_sky_data(data)
        self.assertEqual(result['total_aircraft'], ...)
        # Add more assertions to verify the expected behavior

if __name__ == '__main__':
    unittest.main()