import unittest

from src.services.ovapi_service import Ovapi


class TestOvapiService(unittest.TestCase):
    def test_get_data(self):
        json_data = Ovapi.get_data()

        self.assertTrue("ARR_26007_2" in json_data.keys())


if __name__ == "__main__":
    unittest.main()
