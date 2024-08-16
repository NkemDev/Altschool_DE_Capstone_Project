# CAPSTONE PROJECT: END TO END ETL PROCESS WITH POSTGRES AIRFLOW BIGQUERY AND DBT

## Overview

This project is about developing and end to end ETL process using the popular Brazillian olist dataset.


## Tools used for this project
- Docker compose: This creates a docker container(s) that are used instead of using the local machine to set up the infrastructure
- Postgres: This is the database used to store the airflow and the data csv files.
- Python: This is used to create the dag and the scripts to load the csv file into the database
- Airflow: This is used for orchestrating the transfer of data from postgres to google big query
- Dbt: This is used to create models for transformation of the data from big query
- Big Query: This is a data warehouse for storing data.

## Workflow
- The data files containing csv files of the Brazillian Ecommerce dataset is loaded into postgres database named ecommerce.
- The airflow loads the tables from the ecommerce database into google big query using operators. 
- DBT is then used to create models and transform the data into the desired transformation.

![alt text](/pics/Data%20Orchestration(1).png)

## Data Files
The data files used in the project are:
- olist.customer.csv
- olist.geolocation.csv
- olist.order_items.csv
- olist.orders.csv
- olist.products.csv
- olist.sellers.csv
- product_category_name_translation

These were loaded into postgres database docker container using a docker-compose file.

```
x-airflow-common:

  &airflow-common

  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.1}

  environment:

    &airflow-common-env

    AIRFLOW__CORE__EXECUTOR: LocalExecutor

    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

    AIRFLOW__CORE__FERNET_KEY: ''

    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'

    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'

    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'

    
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
    AIRFLOW__CORE__TEST_CONNECTION: 'ENABLED'
    
  volumes:

    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags

    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs

    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config

    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins

    - ${AIRFLOW_PROJ_DIR:-.}/json:/opt/airflow/json  # Make sure this directory is mapped correctly



  user: "${AIRFLOW_UID:-50000}:0"

  depends_on:

    &airflow-common-depends-on

    postgres:

      condition: service_healthy



services:

  postgres:

    image: postgres:13

    environment:

      POSTGRES_USER: airflow

      POSTGRES_PASSWORD: airflow

      POSTGRES_DB: airflow

   
    volumes:

      - ./postgres-db-volume:/var/lib/postgresql/data

    healthcheck:

      test: ["CMD", "pg_isready", "-U", "airflow"]

      interval: 10s

      retries: 5

      start_period: 5s

    restart: always

  db:
    image: postgres:latest
    container_name: ecommerce_container
    environment:
      POSTGRES_USER: alt_school_user
      POSTGRES_PASSWORD: secretPassw0rd
      POSTGRES_DB: ecommerce
   
    volumes:
      - ./pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "alt_school_user"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always

  pgadmin:

    image: dpage/pgadmin4:latest

    container_name: Capstone_my_pgadmin_container

    restart: always

    environment:

      PGADMIN_DEFAULT_EMAIL: admin@admin.com

      PGADMIN_DEFAULT_PASSWORD: _admin

    ports:

      - "5002:80"

    depends_on:

      db:

        condition: service_healthy



  python-script:

    build:

      context: .

      dockerfile: Dockerfile

    container_name: capstone_python_script_container

    volumes:

      - /var/run/docker.sock:/var/run/docker.sock

      - ./src:/src

      - ./data:/data

    working_dir: /src

    command: python infra_scripts.py

    depends_on:

      db:

        condition: service_healthy

    networks:

      - default



  airflow-webserver:

    <<: *airflow-common

    command: webserver

    ports:

      - "8082:8080"

    healthcheck:

      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]

      interval: 30s

      timeout: 10s

      retries: 5

      start_period: 30s

    restart: always

    depends_on:

      <<: *airflow-common-depends-on

      airflow-init:

        condition: service_completed_successfully



  airflow-scheduler:

    <<: *airflow-common

    command: scheduler

    healthcheck:

      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]

      interval: 30s

      timeout: 10s

      retries: 5

      start_period: 30s

    restart: always

    depends_on:

      <<: *airflow-common-depends-on

      airflow-init:

        condition: service_completed_successfully



  airflow-triggerer:

    <<: *airflow-common

    command: triggerer

    healthcheck:

      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']

      interval: 30s

      timeout: 10s

      retries: 5

      start_period: 30s

    restart: always

    depends_on:

      <<: *airflow-common-depends-on

      airflow-init:

        condition: service_completed_successfully



  airflow-init:

    <<: *airflow-common

    entrypoint: /bin/bash

    command:

      - -c

      - |

        function ver() {

          printf "%04d%04d%04d%04d" $${1//./ }

        }

        airflow_version=$$(AIRFLOW__LOGGING__LOGGING_LEVEL=INFO && gosu airflow airflow version)

        airflow_version_comparable=$$(ver $${airflow_version})

        min_airflow_version=2.2.0

        min_airflow_version_comparable=$$(ver $${min_airflow_version})

        if (( airflow_version_comparable < min_airflow_version_comparable )); then

          echo

          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"

          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"

          echo

          exit 1

        fi

        if [[ -z "${AIRFLOW_UID}" ]]; then

          echo

          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"

          echo "If you are on Linux, you SHOULD follow the instructions below to set "

          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."

          echo "For other operating systems you can get rid of the warning with manually created .env file:"

          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"

          echo

        fi

        one_meg=1048576

        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))

        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)

        disk_available=$$(df / | tail -1 | awk '{print $$4}')

        warning_resources="false"

        if (( mem_available < 4000 )) ; then

          echo

          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"

          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"

          echo

          warning_resources="true"

        fi

        if (( cpus_available < 2 )); then

          echo

          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"

          echo "At least 2 CPUs recommended. You have $${cpus_available}"

          echo

          warning_resources="true"

        fi

        if (( disk_available < one_meg * 10 )); then

          echo

          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"

          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"

          echo

          warning_resources="true"

        fi

        if [[ $${warning_resources} == "true" ]]; then

          echo

          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"

          echo "Please follow the instructions to increase amount of resources available:"

          echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"

          echo

        fi

        mkdir -p /sources/logs /sources/dags /sources/plugins

        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}

        exec /entrypoint airflow version

    environment:

      <<: *airflow-common-env

      _AIRFLOW_DB_MIGRATE: 'true'

      _AIRFLOW_WWW_USER_CREATE: 'true'

      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}

      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}

      _PIP_ADDITIONAL_REQUIREMENTS: ''

    user: "0:0"

    volumes:

      - ${AIRFLOW_PROJ_DIR:-.}:/sources



  airflow-cli:

    <<: *airflow-common

    profiles:

      - debug

    environment:

      <<: *airflow-common-env

      CONNECTION_CHECK_MAX_COUNT: "0"
      
    command:

      - bash

      - -c

      - airflow

   
```
The docker containers that are created with this docker-compose file are:
- airflow webserver: This is the webserver ui for airflow.
- airflow postgres database: This is the database for airflow 
- airflow scheduler: This schedules the data orchestrations
- pgadmin4: This is the postgres server that displays the loaded datafiles 
- db: This is the name of the postgres database that stores the datafiles.
- Dockerfile: This Dockerfile sets up a lightweight Python 3.9 environment for running a script, likely involving PostgreSQL. It uses the slim version of Python as the base image and installs essential build tools and the PostgreSQL development library. The working directory is set to /src/, and the application files are copied into this directory.
```
# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install PostgreSQL development packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev
# Set the working directory in the container
WORKDIR /src/

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.python.org/simple

# Run etl.py when the container launches
CMD ["python", "infra_scripts.py"]

```
`infra_scripts.py`: This Python script processes and loads CSV files into a PostgreSQL database using pandas and SQLAlchemy. It first checks if the /data directory exists and lists the CSV files within. Each CSV file is read into a pandas DataFrame, and its content is loaded into a PostgreSQL database, with the table name derived from the file name. If the directory or files are missing, or if errors occur during reading or writing, appropriate error messages are displayed. The script concludes by confirming that all CSV files have been processed and loaded into the database.

```
import pandas as pd
from sqlalchemy import create_engine
import os

# Use service name 'postgres' for the database URL
URL = "postgresql://alt_school_user:secretPassw0rd@db:5432/ecommerce"

extract_dir = "/data"

engine = create_engine(URL)

if not os.path.exists(extract_dir):
    print(f"Directory {extract_dir} does not exist.")
else:
    print(f"Directory {extract_dir} exists. Loading CSV files...")

    files = os.listdir(extract_dir)
    if not files:
        print(f"No files found in {extract_dir}.")
    else:
        print(f"Files in {extract_dir}: {files}")

for csv_file in files:
    if csv_file.endswith(".csv"):
        file_path = os.path.join(extract_dir, csv_file)

        print(f"Processing file: {file_path}")

        try:
            df = pd.read_csv(file_path)
            print(f"Read {len(df)} rows from {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
            continue

        table_name = os.path.splitext(csv_file)[0]

        try:
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"Data from {csv_file} has been loaded into table {table_name}.")
        except Exception as e:
            print(f"Error writing {table_name} to database: {str(e)}")

print("All CSV files have been processed and loaded into the PostgreSQL database.")
```
run `docker-compose up` this will create the containers and build the docker image

![alt text](/pics/postgresdatabase.png)
This is the view of the loaded datafiles each as a table in postgres database


## Airflow Dag
This is a python script that will orchestrate a task. In this case I want to load the tables from the ecommerce database into bigquery. Here is the dag script `py_to_gcs.py`
```
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    "load_postgres_to_bigquery",
    default_args=default_args,
    description="An airflow DAG to load data from Postgres to Big Query",
    schedule_interval=None,  # Adjust as needed
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    table_list = [
        "olist_customers",
        "olist_geolocation",
        "olist_order_items",
        "olist_order_payments",
        "olist_order_reviews",
        "olist_orders",
        "olist_products",
        "olist_sellers",
        "product_category_name_translation",
    ]

    for table in table_list:
        postgres_to_gcs = PostgresToGCSOperator(
            task_id=f"postgres_to_gcs_{table}",
            postgres_conn_id="postgres_conn",
            sql=f"SELECT * FROM {table}",
            bucket="nkem_capstone_bucket",
            filename=f"{table}.json",
            export_format="json",
        )

        gcs_to_bigquery = GCSToBigQueryOperator(
            task_id=f"gcs_to_bigquery_{table}",
            bucket="nkem_capstone_bucket",
            source_objects=[f"{table}.json"],
            destination_project_dataset_table=f"luminous-pier-424818-g0.olist_datasets.{table}",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        start >> postgres_to_gcs >> gcs_to_bigquery >> end
```
Before this works you have to set up connections from Airflow to Postgres and Airflow to GCP. Make there is a service account with the appropriate permissions to write and read files


![alt text](/pics/postgrescon.png)


![alt text](/pics/google_cloud.png)


The connection is then tested to see if it works correctly.
When the dag is created and started on the webserver. This will the load the data from postgres into bigquery

![alt text](/pics/airflowgraph.png)

## Data Transformation with DBT
DBT builds and transform models into the desired specification. According to the questions required for this project, out of the nine tables given only four will be required to be changed to staging models
![alt text](/pics/dbt_1.png)

Staging models are one-to-one mapping with their sources and the materialization is in views.
The intermediate models could be transformng of a staging models or joining of two or more models to make another model. 
The final model is a fine tuning of the intermediate model
## Result Report of Final Model

![alt text](/pics/correcttime.png)
