from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='master_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval='@daily',  # We can schedule the whole process now
    catchup=False,
    tags=['master', 'data-engineering'],
) as dag:
    # Task to trigger the schema creation DAG
    trigger_schema_creation = TriggerDagRunOperator(
        task_id='trigger_create_warehouse_schema',
        trigger_dag_id='create_warehouse_schema',  # The dag_id of the DAG to trigger
        wait_for_completion=True,  # Wait until the triggered DAG is done
    )

    # Task to trigger the customer processing DAG
    trigger_customer_pipeline = TriggerDagRunOperator(
        task_id='trigger_customer_processing_pipeline',
        trigger_dag_id='customer_processing_pipeline',
        wait_for_completion=True,
    )

    # Task to trigger the product processing DAG
    trigger_product_pipeline = TriggerDagRunOperator(
        task_id='trigger_product_processing_pipeline',
        trigger_dag_id='product_processing_pipeline',
        wait_for_completion=True,
    )

    # Task to trigger the orders processing DAG
    trigger_orders_pipeline = TriggerDagRunOperator(
        task_id='trigger_orders_processing_pipeline',
        trigger_dag_id='orders_processing_pipeline',
        wait_for_completion=True,
    )

    # Define the execution order
    trigger_schema_creation >> [trigger_customer_pipeline, trigger_product_pipeline] >> trigger_orders_pipeline
