import unittest
from dags.ovapi_pipeline import check_line


class TestCheckLine(unittest.TestCase):
    def test_no_error(self):
        line_name = "AVV_100804_1"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DataOwnerCode": "AVV",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "100804",
            "LineName": "",
            "LineDirection": 1,
        }

        result = check_line(line_name, line_data)

        assert result == {}

    def test_missing_DataOwnerCode(self):
        line_name = "AVV_100804_1"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "100804",
            "LineName": "",
            "LineDirection": 1,
        }

        result = check_line(line_name, line_data)

        assert result == {"DataOwnerCode": ["required field"]}

    def test_empty_LinePlanningNumber(self):
        line_name = "AVV_100804_1"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DataOwnerCode": "AVV",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "",
            "LineName": "",
            "LineDirection": 1,
        }

        result = check_line(line_name, line_data)

        assert result == {"LinePlanningNumber": ["empty values not allowed"]}

    def test_null_LineDirection(self):
        line_name = "AVV_100804_1"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DataOwnerCode": "AVV",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "100804",
            "LineName": "",
            "LineDirection": None,
        }

        result = check_line(line_name, line_data)

        assert result == {"LineDirection": ["null value not allowed"]}

    def test_additional_data(self):
        line_name = "AVV_100804_1"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DataOwnerCode": "AVV",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "100804",
            "LineName": "",
            "LineDirection": 1,
            "AdditionalData": "Is ignored",
        }

        result = check_line(line_name, line_data)

        assert result == {}

    def test_missing_non_necessary_data(self):
        line_name = "AVV_100804_1"
        line_data = {
            "DataOwnerCode": "AVV",
            "LinePlanningNumber": "100804",
            "LineDirection": 1,
        }

        result = check_line(line_name, line_data)

        assert result == {}

    def test_incorrect_line_name(self):
        line_name = "incorrect"
        line_data = {
            "LineWheelchairAccessible": "UNKNOWN",
            "TransportType": "BUS",
            "DestinationName50": "Schmithof Schule",
            "DataOwnerCode": "AVV",
            "DestinationCode": "1884",
            "LinePublicNumber": "V",
            "LinePlanningNumber": "100804",
            "LineName": "",
            "LineDirection": 1,
        }

        result = check_line(line_name, line_data)

        assert result == {
            "line_name": ["should be 'AVV_100804_1' (actual :'incorrect')"]
        }


if __name__ == "__main__":
    unittest.main()
