import datetime
import json
import logging
from textwrap import dedent

from cerberus import Validator

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger("airflow.task")


# Define utils functions


def check_line(line_data: dict, line_name: str = None) -> dict:
    """Checks an OV line data.

    Uses [Cerberus](https://docs.python-cerberus.org/en/stable/index.html) to
    define schema specifications that actual data will be validated against.
    Validation rules must match the `line` table schema definition.
    Also checks `line_name` consistency with `line_data`.

    Args:
        line_data (dict): Line data (ex. : {
            "LineWheelchairAccessible":"UNKNOWN",
            "TransportType":"BUS",
            "DestinationName50":"Schmithof Schule",
            "DestinationCode":"1884",
            "LinePublicNumber":"V",
            "LinePlanningNumber":"100804",
            "LineName":"",
            "LineDirection":1
        })
        line_name (str): Line name (ex. : "AVV_100804_1")

    Returns:
        dict: List of errors per field (ex. : {"DataOwnerCode": ["required field"]})
    """

    # Validation rules definition
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

    # Validate line data
    line_validator.validate(line_data)
    errors = line_validator.errors

    if line_name is not None:
        # Validate line name
        expected_line_name = f"{line_data.get('DataOwnerCode', None)}_{line_data.get('LinePlanningNumber', None)}_{line_data.get('LineDirection', None)}"
        if not line_name == expected_line_name:
            errors["line_name"] = [
                f"should be '{expected_line_name}' (actual :'{line_name}')"
            ]

    return errors


def build_upsert_query(line_data: dict) -> str:
    """Builds an upsert PostgreSQL query from valid OV line data.

    Upsert only the columns present in the line data, and leave the other
        columns empty/unchanged.
    Update only rows that have not been updated since current DAG run (ts),
        in case of a re-run.

    Returns a templated "INSERT ON CONFLICT DO UPDATE" SQL query.
    https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT

    Args:
        line_data (dict): Line data (ex. : {
            "LineWheelchairAccessible":"UNKNOWN",
            "TransportType":"BUS",
            "DestinationName50":"Schmithof Schule",
            "DestinationCode":"1884",
            "LinePublicNumber":"V",
            "LinePlanningNumber":"100804",
            "LineName":"",
            "LineDirection":1
        })

    Returns:
        str: Upsert PostgreSQL query (ex. :
            INSERT INTO line ( LineWheelchairAccessible, TransportType, DestinationName50, DataOwnerCode, DestinationCode, LinePublicNumber, LinePlanningNumber, LineName, LineDirection, created_at, updated_at)
            VALUES ( 'UNKNOWN', 'BUS', 'Schmithof Schule', 'AVV', '1884', 'V', '100804', '', '1', {{ ts }}, {{ ts }} )
            ON CONFLICT ("DataOwnerCode", "LinePlanningNumber", "LineDirection") DO UPDATE
            SET (
                LineWheelchairAccessible = EXCLUDED.LineWheelchairAccessible, TransportType = EXCLUDED.TransportType, DestinationName50 = EXCLUDED.DestinationName50, DataOwnerCode = EXCLUDED.DataOwnerCode, DestinationCode = EXCLUDED.DestinationCode, LinePublicNumber = EXCLUDED.LinePublicNumber, LinePlanningNumber = EXCLUDED.LinePlanningNumber, LineName = EXCLUDED.LineName, LineDirection = EXCLUDED.LineDirection,
                updated_at = {{ ts }}
            )
            WHERE updated_at < {{ ts }};
        )
    """
    # Make sure the line is valid (defensive programming)
    if line_errors := check_line(line_data):
        raise ValueError(
            f"Line data is not valid : {json.dumps(line_errors, indent=4)}"
        )

    # @TODO : Should be using prepared statement
    # Build the attributes and values list
    insert_attributes = ", ".join(line_data.keys())
    insert_values = ", ".join([f"'{v}'" for v in line_data.values()])
    update_attributes = ", ".join([f"{k} = EXCLUDED.{k}" for k in line_data.keys()])

    # Bandit workaround : https://github.com/PyCQA/bandit/issues/658#issuecomment-1035095777
    # fmt: off
    return (  # nosec
        f"""
            INSERT INTO line ( {insert_attributes}, created_at, updated_at)
            VALUES ( {insert_values}, {{{{ ts }}}}, {{{{ ts }}}} )
            ON CONFLICT ("DataOwnerCode", "LinePlanningNumber", "LineDirection") DO UPDATE
            SET (
                {update_attributes},
                updated_at = {{{{ ts }}}}
            )
            WHERE updated_at < {{{{ ts }}}};
        """  # nosec
    )
    # fmt: on


# Define the DAG


@dag(
    dag_id="ovapi-pipeline",
    schedule="@daily",
    start_date=datetime.datetime(
        2000,
        1,
        1,
    ),
    tags=["ovapi"],
)
def ovapi_pipeline():
    """Daily copy data from OV Lines API to PostgreSQL table.

    We use Airflow 2.0 's TaskFlow API paradigm to define the DAG and Tasks.
    Doc : https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html

    - init : create PostreSQL table if not exists
    - extract : get OV Lines data from public API
    - transform : clean the data
    - load : upsert clean data to PostreSQL DB
    """

    # init : create PostreSQL table if not exists
    init = PostgresOperator(
        task_id="ovapi-init",
        sql="sql/line_schema.sql",
        # @TODO : define and use a specific connection
    )
    init.doc_md = dedent(
        """Init task

        Uses PostgresOperator to execute sql/line_schema.sql
        Doc : https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html

        Simply execute the "CREATE TABLE IF NOT EXISTS `line` ..."
        Composite primary key : (`DataOwnerCode`, `LinePlanningNumber`, `LineDirection`)
        """
    )

    # extract : get OV Lines data from public API
    @task(task_id="ovapi-extract")
    def extract() -> dict:
        """Returns data from OV's API.

        OV API doc : https://github.com/koch-t/KV78Turbo-OVAPI/wiki/Line

        @TODO : create a Airflow connection and use SimpleHttpOperator
        https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.html#simplehttpoperator

        Returns:
            dict: list of available lines (ex. : {
                "AVV_100804_1":{
                    "LineWheelchairAccessible":"UNKNOWN",
                    "TransportType":"BUS",
                    "DestinationName50":"Schmithof Schule",
                    "DataOwnerCode":"AVV",
                    "DestinationCode":"1884",
                    "LinePublicNumber":"V",
                    "LinePlanningNumber":"100804",
                    "LineName":"",
                    "LineDirection":1
                },
                "ARR_28167_2":{
                    "LineWheelchairAccessible":"NOTACCESSIBLE",
                    "TransportType":"BUS",
                    "DestinationName50":"Alde Leie Brug",
                    "DataOwnerCode":"ARR",
                    "DestinationCode":"2671",
                    "LinePublicNumber":"7911",
                    "LinePlanningNumber":"28167",
                    "LineName":"Alde Leie - Stiens",
                    "LineDirection":2
                },
                ...)
        """
        import requests

        OVAPI_URL: str = "http://v0.ovapi.nl/line"
        response = requests.get(OVAPI_URL)
        json_data = response.json()

        return json_data

    # transform : clean the data
    @task(task_id="ovapi-transform")
    def transform(json_data: dict) -> list:
        """Validates each line.

        Calls check_line for each line.
        If the line is valid
            => appends the line data to valid_lines list
        Else
            => loggs a Warning message

        Args:
            json_data (dict): list of lines {line_name: line_data, ... }

        Returns:
            list: list of valid lines data (ex. : [
                {
                    "LineWheelchairAccessible":"NOTACCESSIBLE",
                    "TransportType":"BUS",
                    "DestinationName50":"Alde Leie Brug",
                    "DataOwnerCode":"ARR",
                    "DestinationCode":"2671",
                    "LinePublicNumber":"7911",
                    "LinePlanningNumber":"28167",
                    "LineName":"Alde Leie - Stiens",
                    "LineDirection":2
                },
                ...
            ])
        """
        valid_lines = []
        for line_name, line_data in json_data:
            # check each lines validity
            line_errors = check_line(line_data, line_name)
            if line_errors:
                # line has errors => log a Warning (to be investigated)
                task_logger.warning(
                    f"Invalid line '{line_name}': {json.dumps(line_errors, indent=4)}"
                )
            else:
                # line has no errors => add to list of valid lines
                valid_lines.append(line_data)

        return valid_lines

    # load : upsert clean data to PostreSQL DB
    @task(task_id="ovapi-load")
    def load(valid_lines: list):
        """Upsert the valid lines into `line` table.

        Uses PostgresHook to execute the upsert query built with build_upsert_query
        Doc : https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html#airflow.providers.postgres.hooks.postgres.PostgresHook

        The upserts are commited all at once.

        Args:
            valid_lines (list): _description_
        """
        # @TODO : define and use a specific connection
        postgres_hook = PostgresHook()
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for valid_line in valid_lines:
            query = build_upsert_query(valid_line)
            cur.execute(query)

        conn.commit()

    # Define the DAG edges
    json_data = extract()
    valid_lines = transform(json_data)
    init >> load(valid_lines)


# Enable the DAG
ovapi_pipeline()
