from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='customer_processing_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval='@once',
    tags=['spark', 'data-engineering'],
) as dag:
    # This is a command that tells Docker to run our spark-submit job
    # directly inside the spark-master container.
    # This method is simple and avoids all networking issues.
    spark_submit_command = """
    docker exec spark-master spark-submit \
      --master spark://spark-master:7077 \
      --packages org.postgresql:postgresql:42.5.0 \
      /opt/bitnami/spark/jobs/transform_customers.py
    """

    # We use a BashOperator to run our simple command.
    process_customers = BashOperator(
        task_id='submit_customer_spark_job',
        bash_command=spark_submit_command,
    )