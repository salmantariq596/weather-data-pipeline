"""
Unit tests for the ETL pipeline
"""
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import json

import os
import sys
import tempfile
os.environ.setdefault('RAW_DATA_DIR', os.path.join(tempfile.gettempdir(), 'weather_raw_data'))
sys.path.insert(0, '/app')  # inside the Docker image
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))  # local checkout

from etl_full import (
    DataQualityChecker,
    WeatherTransformer,
    RawDataManager,
    WeatherExtractor,
    Config
)


class TestDataQualityChecker(unittest.TestCase):
    def test_valid_temperature(self):
        ok, msg = DataQualityChecker.check_temperature(25.0)
        self.assertTrue(ok)
        self.assertIsNone(msg)

    def test_invalid_temperature_too_high(self):
        ok, msg = DataQualityChecker.check_temperature(70.0)
        self.assertFalse(ok)
        self.assertIn("out of realistic range", msg)

    def test_null_temperature(self):
        ok, msg = DataQualityChecker.check_temperature(None)
        self.assertFalse(ok)
        self.assertIn("null", msg)

    def test_quality_score_perfect(self):
        record = {"temperature": 25.0, "humidity": 50, "pressure": 1013}
        score = DataQualityChecker.calculate_quality_score(record)
        self.assertEqual(score, 1.0)

    def test_quality_score_partial(self):
        record = {"temperature": 25.0, "humidity": None, "pressure": 1013}
        score = DataQualityChecker.calculate_quality_score(record)
        self.assertEqual(score, 0.67)


class TestWeatherTransformer(unittest.TestCase):
    def test_transform_openweather_current(self):
        raw_data = {
            "main": {"temp": 30.5, "feels_like": 32.0, "temp_min": 28.0, "temp_max": 33.0,
                     "pressure": 1013, "humidity": 65},
            "wind": {"speed": 5.2, "deg": 180, "gust": 8.5},
            "weather": [{"main": "Clear", "description": "clear sky", "icon": "01d"}],
            "clouds": {"all": 10},
            "sys": {"country": "PK", "sunrise": 1609459200, "sunset": 1609502400},
            "coord": {"lat": 29.3956, "lon": 71.6836},
            "name": "Bahawalpur",
            "timezone": 18000,
            "visibility": 10000
        }

        result = WeatherTransformer.transform_openweather_current(
            "test-uuid", raw_data, datetime.now(timezone.utc)
        )

        self.assertEqual(result["temperature"], 30.5)
        self.assertEqual(result["humidity_percent"], 65)
        self.assertEqual(result["weather_main"], "Clear")
        self.assertEqual(result["source"], "openweather")
        self.assertIsNotNone(result["data_quality_score"])


class TestRawDataManager(unittest.TestCase):
    def test_fingerprint_generation(self):
        data = {"source": "test", "api_endpoint": "current", "data": {"temp": 25}}
        manager = RawDataManager()
        fp1 = manager._generate_fingerprint(data)
        fp2 = manager._generate_fingerprint(data)
        self.assertEqual(fp1, fp2)

        # Different data should produce different fingerprint
        data2 = {"source": "test", "api_endpoint": "current", "data": {"temp": 26}}
        fp3 = manager._generate_fingerprint(data2)
        self.assertNotEqual(fp1, fp3)


class TestWeatherExtractor(unittest.TestCase):
    @patch('etl_full.requests.Session')
    def test_extract_openweather_current(self, mock_session):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"main": {"temp": 25}}
        mock_session.return_value.get.return_value = mock_response

        extractor = WeatherExtractor()
        import etl_full
        with patch.object(etl_full.CONFIG, 'OPENWEATHER_API_KEY', 'test_key'):
            result = extractor.extract_openweather_current()

        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "openweather")


if __name__ == '__main__':
    unittest.main()
