from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id='create_warehouse_schema',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval='@once',
    tags=['setup', 'data-warehouse'],
) as dag:
    # Task to create the tables and apply schema definitions
    create_schema_task = PostgresOperator(
        task_id='execute_create_schema_script',
        # This points to the default postgres connection that Airflow creates
        postgres_conn_id='postgres_default',
        # The path to our SQL file relative to the DAGs folder
        sql='sql/create_warehouse_schema.sql'
    )
