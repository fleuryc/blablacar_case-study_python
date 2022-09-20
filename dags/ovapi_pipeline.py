import datetime
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger("airflow.task")


@dag(
    dag_id="ovapi-pipeline",
    schedule="@daily",
    start_date=datetime.datetime(
        2000,
        1,
        1,
    ),
)
def ovapi_pipeline():
    """_summary_

    Returns:
        _type_: _description_
    """
    create_line_table = PostgresOperator(
        task_id="create_line_table",
        sql="sql/line_schema.sql",
    )

    @task
    def extract() -> dict:
        """Returns data from OV's API.

        Returns:
            dict: list of available lines
        """
        import requests

        OVAPI_URL: str = "http://v0.ovapi.nl/line"
        response = requests.get(OVAPI_URL)
        json_data = response.json()

        return json_data

    @task
    def transform(json_data: dict) -> list:
        """_summary_

        Args:
            json_data (dict): _description_

        Returns:
            dict: _description_
        """
        import json

        from cerberus import Validator

        line_schema = {
            "DataOwnerCode": {
                "type": "string",
                "maxlength": 255,
                "required": True,
                "empty": False,
            },
            "LinePlanningNumber": {
                "type": "string",
                "maxlength": 255,
                "required": True,
                "empty": False,
            },
            "LineDirection": {
                "type": "integer",
                "required": True,
                "min": 0,
                "max": 32767,
            },
            "LinePublicNumber": {"type": "string", "maxlength": 255},
            "LineName": {"type": "string", "maxlength": 255},
            "DestinationName50": {"type": "string", "maxlength": 255},
            "DestinationCode": {"type": "string", "maxlength": 255},
            "LineWheelchairAccessible": {"type": "string", "maxlength": 255},
            "TransportType": {"type": "string", "maxlength": 255},
        }
        line_validator = Validator(line_schema, purge_unknown=True)

        valid_lines = []
        for line_name, line_data in json_data:
            if not line_validator.validate(line_data):
                task_logger.warning(
                    f"Invalid line '{line_name}': {json.dumps(line_validator.errors, indent=4)}"
                )
            else:
                valid_lines.append(line_data)

        return valid_lines

    @task
    def load(valid_lines: list):
        postgres_hook = PostgresHook()
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for valid_line in valid_lines:
            query = f"""
                INSERT INTO line ( {', '.join(valid_line.keys())}, created_at, updated_at)
                VALUES ( {', '.join(valid_line.values())}, NOW(), NOW() );
                ON CONFLICT ("DataOwnerCode", "LinePlanningNumber", "LineDirection") DO UPDATE
                SET (
                    {', '.join([f"{k} = EXCLUDED.{k}" for k in valid_line.keys()])},
                    updated_at = NOW(),
                );
            """
            cur.execute(query)

        conn.commit()

    json_data = extract()
    valid_lines = transform(json_data)
    create_line_table >> load(valid_lines)


dag = ovapi_pipeline()
