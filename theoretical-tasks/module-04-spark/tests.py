import unittest
from pyspark.sql import SparkSession
from udfs import get_coordinate, get_geohash


class TestGeoFunctions(unittest.TestCase):
    def test_get_coordinate(self):
        # Positive test case: Valid coordinates
        result = get_coordinate("Savoria", "Dillon", "US")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], float)
        self.assertIsInstance(result[1], float)

        # Negative test case: Invalid franchise and city
        result = get_coordinate("InvalidFranchise", "InvalidCity", "ZZ")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])

        # Negative test case: Invalid country code
        result = get_coordinate("Savoria", "Dillon", "InvalidCountry")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result[0])
        self.assertIsNotNone(result[1])

        # Negative test case: Missing franchise name
        result = get_coordinate("", "Dillon", "US")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result[0])
        self.assertIsNotNone(result[1])

        # Negative test case: Missing city
        result = get_coordinate("Savoria", "", "US")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])

    def test_get_geohash(self):
        # Positive test case: Valid coordinates
        result = get_geohash(1, -1)
        self.assertIsInstance(result, str)
        self.assertEqual(len(result), 4)

        # Negative test case: Missing latitude or longitude
        result = get_geohash(None, -1)
        self.assertIsNone(result)

        # Negative test case: Out of bound latitude or longitude
        result = get_geohash(181, -181)
        self.assertIsNone(result)

        # Positive test case: Coordinates within a specific range
        result = get_geohash(39.63026, -106.04335)
        self.assertIsInstance(result, str)
        self.assertEqual(len(result), 4)
        self.assertTrue(result.isalnum())  # Check if the result is alphanumeric


if __name__ == "__main__":
    unittest.main()
