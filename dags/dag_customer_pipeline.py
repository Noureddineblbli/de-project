from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    dag_id='customer_processing_pipeline',
    default_args=default_args,
    description='A pipeline to process customer data with Spark',
    schedule_interval='@once',  # This DAG will run once manually triggered
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,  # Do not perform back-runs for past dates
    tags=['spark', 'data-engineering'],
) as dag:
    # Define the Spark Submit task
    # This task will run our PySpark job.
    process_customers = SparkSubmitOperator(
        task_id='submit_customer_spark_job',
        application='/opt/bitnami/spark/jobs/transform_customers.py',
        conn_id='spark_default',
        # Add this line to provide the PostgreSQL JDBC driver to Spark
        packages='org.postgresql:postgresql:42.5.0',
        verbose=False,
    )
