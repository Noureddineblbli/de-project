from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DateType


def main():
    """
    Main ETL script for processing orders data.
    """
    spark = SparkSession.builder \
        .appName("OrdersDataProcessing") \
        .getOrCreate()

    # --- EXTRACT ---
    source_path = "/opt/bitnami/spark/data/orders.csv"
    df_orders = spark.read.csv(source_path, header=True, inferSchema=True)

    print("Source data schema:")
    df_orders.printSchema()

    # --- TRANSFORM ---
    # 1. Handle missing 'quantity' and cast to IntegerType
    df_transformed = df_orders.withColumn(
        "quantity",
        when(col("quantity").isNull(), 1).otherwise(col("quantity")).cast(IntegerType())
    )

    # 2. Cast 'order_date' to DateType
    df_transformed = df_transformed.withColumn(
        "order_date",
        col("order_date").cast(DateType())
    )

    # NOTE: We are intentionally NOT cleaning up the invalid customer_id=99 for now.
    # We will use this to demonstrate a final, combined pipeline run later.

    print("Transformed data schema:")
    df_transformed.printSchema()
    df_transformed.show()

    # --- DATA QUALITY CHECKS ---
    print("Performing data quality checks...")

    # Check for nulls in the primary key column
    null_count = df_transformed.filter(col("order_id").isNull()).count()
    if null_count > 0:
        print(f"Data Quality Check FAILED: Found {null_count} nulls in order_id.")
        raise ValueError("Data quality check failed: order_id contains null values.")

    print("Data Quality Check PASSED: No nulls in order_id.")

    # --- LOAD ---
    db_url = "jdbc:postgresql://postgres-dw:5432/dw_database"
    db_properties = {
        "user": "dw_user",
        "password": "dw_password",
        "driver": "org.postgresql.Driver"
    }
    table_name = "fact_orders"

    # Write the DataFrame to the new PostgreSQL table
    df_transformed.write.jdbc(
        url=db_url,
        table=table_name,
        mode="append",
        properties=db_properties
    )

    print(f"Successfully loaded data into '{table_name}' table.")

    spark.stop()


if __name__ == "__main__":
    main()
