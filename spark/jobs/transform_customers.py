from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DateType


def main():
    """
    Main ETL script for processing customer data.
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CustomerDataProcessing") \
        .getOrCreate()

    # --- EXTRACT ---
    # The path corresponds to the volume mount in our docker-compose.yaml
    source_path = "/opt/bitnami/spark/data/customers.csv"
    df_customers = spark.read.csv(source_path, header=True, inferSchema=True)

    print("Source data schema:")
    df_customers.printSchema()
    print("Source data sample:")
    df_customers.show(5)

    # --- TRANSFORM ---
    # 1. Handle nulls in 'first_name'
    df_transformed = df_customers.withColumn(
        "first_name",
        when(col("first_name") == "null", "Unknown").otherwise(col("first_name"))
    )

    # 2. Handle nulls/blanks in 'email'
    df_transformed = df_transformed.withColumn(
        "email",
        when(col("email").isNull(), "no-email@provided.com").otherwise(col("email"))
    )

    # 3. Cast 'registration_date' to DateType
    df_transformed = df_transformed.withColumn(
        "registration_date",
        col("registration_date").cast(DateType())
    )

    print("Transformed data schema:")
    df_transformed.printSchema()
    print("Transformed data sample:")
    df_transformed.show(5)

    # --- DATA QUALITY CHECKS ---
    print("Performing data quality checks...")

    # Check for nulls in the primary key column
    null_count = df_transformed.filter(col("customer_id").isNull()).count()
    if null_count > 0:
        print(f"Data Quality Check FAILED: Found {null_count} nulls in customer_id.")
        raise ValueError("Data quality check failed: customer_id contains null values.")

    print("Data Quality Check PASSED: No nulls in customer_id.")

    # --- LOAD ---
    # Define PostgreSQL connection properties
    # The hostname 'postgres-dw' is the service name from our docker-compose.yaml
    db_url = "jdbc:postgresql://postgres-dw:5432/dw_database"
    db_properties = {
        "user": "dw_user",
        "password": "dw_password",
        "driver": "org.postgresql.Driver"
    }
    table_name = "dim_customer"

    # Write the DataFrame to the PostgreSQL table
    # 'overwrite' mode will drop and recreate the table each time, which is fine for development
    df_transformed.write.jdbc(
        url=db_url,
        table=table_name,
        mode="overwrite",
        properties=db_properties
    )

    print(f"Successfully loaded data into '{table_name}' table.")

    # Stop the Spark Session
    spark.stop()


if __name__ == "__main__":
    main()
