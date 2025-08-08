from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='product_processing_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['spark', 'data-engineering'],
) as dag:
    # Command to execute the Spark job inside the spark-master container
    spark_submit_command = """
    docker exec spark-master spark-submit \
      --master spark://spark-master:7077 \
      --packages org.postgresql:postgresql:42.5.0 \
      /opt/bitnami/spark/jobs/transform_products.py
    """

    # BashOperator to run the spark-submit command
    process_products = BashOperator(
        task_id='submit_product_spark_job',
        bash_command=spark_submit_command,
    )
