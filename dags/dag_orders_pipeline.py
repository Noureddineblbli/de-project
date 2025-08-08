from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='orders_processing_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval='@once',
    tags=['spark', 'data-engineering'],
) as dag:
    # Command to execute the Spark job for orders
    spark_submit_command = """
    docker exec spark-master spark-submit \
      --master spark://spark-master:7077 \
      --packages org.postgresql:postgresql:42.5.0 \
      /opt/bitnami/spark/jobs/transform_orders.py
    """

    # BashOperator to run the spark-submit command
    process_orders = BashOperator(
        task_id='submit_orders_spark_job',
        bash_command=spark_submit_command,
    )
