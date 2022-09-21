# BlaBlaCar - Confirmed Data Engineer Case Study

[![Python application](https://github.com/fleuryc/blablacar_case-study_python/actions/workflows/python-app.yml/badge.svg)](https://github.com/fleuryc/blablacar_case-study_python/actions/workflows/python-app.yml)

This repository contains my submition to BlaBlaCar's case study, as part of the hiring process for a Data Engineering position.

The goal is to implement a data processing pipeline :

- fetch data from an Open API
- store the data in a database
- schedule the pipeline execution with an Airflow DAG

---

**Table of Contents**

- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Virtual environment](#virtual-environment)
  - [Dependencies](#dependencies)
  - [Environment variables](#environment-variables)
- [Usage](#usage)
  - [Quality Assurance](#quality-assurance)

---

## Installation

### Prerequisites

- [Python 3.10](https://www.python.org/downloads/)

### Virtual environment

```bash
# python -m venv env
# > or just :
make venv
source env/bin/activate
```

### Dependencies

```bash
# pip install requests cerberus
# > or :
# pip install -r requirements.txt
# > or just :
make install
```

## Usage

### Quality Assurance

```bash
# make isort
# make format
# make lint
# make bandit
# make mypy
# make test
# > or just :
make qa
```

### Airflow

#### DAG files

- `dags/ovapi_pipeline.py` : DAG definition file
  - `check_line()` : Top-level function to check if a line data is valid
  - `build_upsert_query()` : Top-level function to build the upsert statement of a valid line data
  - `ovapi_pipeline()` : DAG definition function (follows Airflo 2.0 's TaskFlow API paradigm)
    - `init` : PostgresOperator Task to create the target DB table (uses `line_schema.sql`)
    - `extract` : PythonOperator Task to fetch lines data from OV's API
    - `transform` : PythonOperator Task to keep only valid OV lines
    - `load` : PythonOperator Task using PostgresHook to upsert lines data into DB
- `dags/sql/line_schema.sql` : SQL query to create the `line` table if not exists

#### Test files

- `tests/test_ovapi_pipeline.py` : DAG tests file
  - `TestDAG` : TestCase Class to check DAG definition validity
  - `TestCheckLine` : TestCase Class to test `check_line()` function
  - `TestBuildUpsertQuery` : TestCase Class to test `build_upsert_query()` function

#### Run the DAG

Just copy the content of the `dags` folder to your Airflow instance's `dags_folder`.
