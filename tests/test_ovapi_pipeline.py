import unittest
from textwrap import dedent

import pytest

from airflow.models import DagBag
from dags.ovapi_pipeline import build_upsert_query, check_line


@pytest.mark.filterwarnings(
    "ignore:This class is deprecated. Please use `airflow.utils.task_group.TaskGroup`."
)
class TestDAG(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id="ovapi-pipeline")

    def test_dag_loaded(self):
        assert self.dagbag.import_errors == {}
        assert self.dag is not None

    def test_dag_tasks(self):
        expected_dag_tasks = {
            "ovapi-init": ["ovapi-load"],
            "ovapi-extract": ["ovapi-transform"],
            "ovapi-transform": ["ovapi-load"],
            "ovapi-load": [],
        }

        assert len(self.dag.tasks) == len(expected_dag_tasks)

        for task_id, downstream_list in expected_dag_tasks.items():
            assert self.dag.has_task(task_id)
            task = self.dag.get_task(task_id)
            assert task.downstream_task_ids == set(downstream_list)


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

        result = check_line(line_data, line_name)

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

        result = check_line(line_data, line_name)

        assert result == {
            "DataOwnerCode": ["required field"],
            "line_name": ["should be 'None_100804_1' (actual :'AVV_100804_1')"],
        }

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

        result = check_line(line_data, line_name)

        assert result == {
            "LinePlanningNumber": ["empty values not allowed"],
            "line_name": ["should be 'AVV__1' (actual :'AVV_100804_1')"],
        }

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

        result = check_line(line_data, line_name)

        assert result == {
            "LineDirection": ["null value not allowed"],
            "line_name": ["should be 'AVV_100804_None' (actual :'AVV_100804_1')"],
        }

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

        result = check_line(line_data, line_name)

        assert result == {}

    def test_missing_non_necessary_data(self):
        line_name = "AVV_100804_1"
        line_data = {
            "DataOwnerCode": "AVV",
            "LinePlanningNumber": "100804",
            "LineDirection": 1,
        }

        result = check_line(line_data, line_name)

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

        result = check_line(line_data, line_name)

        assert result == {
            "line_name": ["should be 'AVV_100804_1' (actual :'incorrect')"]
        }


class TestBuildUpsertQuery(unittest.TestCase):
    def test_no_error(self):
        valid_line = {
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

        result = build_upsert_query(valid_line)

        assert dedent(result) == dedent(
            """
            INSERT INTO line ( LineWheelchairAccessible, TransportType, DestinationName50, DataOwnerCode, DestinationCode, LinePublicNumber, LinePlanningNumber, LineName, LineDirection, created_at, updated_at)
            VALUES ( 'UNKNOWN', 'BUS', 'Schmithof Schule', 'AVV', '1884', 'V', '100804', '', '1', {{ ts }}, {{ ts }} )
            ON CONFLICT ("DataOwnerCode", "LinePlanningNumber", "LineDirection") DO UPDATE
            SET (
                LineWheelchairAccessible = EXCLUDED.LineWheelchairAccessible, TransportType = EXCLUDED.TransportType, DestinationName50 = EXCLUDED.DestinationName50, DataOwnerCode = EXCLUDED.DataOwnerCode, DestinationCode = EXCLUDED.DestinationCode, LinePublicNumber = EXCLUDED.LinePublicNumber, LinePlanningNumber = EXCLUDED.LinePlanningNumber, LineName = EXCLUDED.LineName, LineDirection = EXCLUDED.LineDirection,
                updated_at = {{ ts }}
            )
            WHERE updated_at < {{ ts }};
        """
        )


if __name__ == "__main__":
    unittest.main()
