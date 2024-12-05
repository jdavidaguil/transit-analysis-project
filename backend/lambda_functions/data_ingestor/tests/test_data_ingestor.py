import unittest
from unittest.mock import patch
from backend.lambda_functions.data_ingestor.lambda_function import process_open_sky_data

# FILE: backend/lambda_functions/data_ingestor/tests/test_lambda_function.py


class TestProcessOpenSkyData(unittest.TestCase):

    def test_process_open_sky_data_valid(self):
        # Mock valid data
        mock_data = {
            "time": 1684730400,
            "states": [
                ["a5e679", "N48CG   ", "United States", None, None, 35.133, -81.1076],
                ["ace5bb", "DAL3177 ", "United States", None, None, 35.2208, -81.0674]
            ]
        } 

        result = process_open_sky_data(mock_data)

        self.assertEqual(result['total_aircraft'], 2)
        self.assertEqual(len(result['aircraft_data']), 2)

        for aircraft in result['aircraft_data']:
            self.assertIn('icao24', aircraft)
            self.assertIn('callsign', aircraft)
            self.assertIn('origin_country', aircraft)
            self.assertIn('latitude', aircraft)
            self.assertIn('longitude', aircraft)

    def test_process_open_sky_data_invalid(self):
        # Mock invalid data
        mock_data = {
            "time": 1684730400,
            "states": [
                ["invalid_data", "invalid_callsign", "invalid_country", None, None, None, None]
            ]
        }

        result = process_open_sky_data(mock_data)

        self.assertEqual(result['total_aircraft'], 0)
        self.assertEqual(len(result['aircraft_data']), 0)

    def test_process_open_sky_data_exception(self):
        # Mock data that causes an exception
        mock_data = None

        result = process_open_sky_data(mock_data)

        self.assertIn('error', result)

if __name__ == '__main__':
    unittest.main()