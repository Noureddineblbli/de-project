from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType


def main():
    """
    Main ETL script for processing product data.
    """
    spark = SparkSession.builder \
        .appName("ProductDataProcessing") \
        .getOrCreate()

    # --- EXTRACT ---
    source_path = "/opt/bitnami/spark/data/products.csv"
    df_products = spark.read.csv(source_path, header=True, inferSchema=True)

    print("Source data schema:")
    df_products.printSchema()

    # --- TRANSFORM ---
    # 1. Handle nulls in 'category'
    df_transformed = df_products.withColumn(
        "category",
        when(col("category").isNull() | (col("category") == "null"), "Uncategorized").otherwise(col("category"))
    )

    # 2. Handle missing 'price' and cast to DoubleType
    df_transformed = df_transformed.withColumn(
        "price",
        when(col("price").isNull(), 0.0).otherwise(col("price")).cast(DoubleType())
    )

    print("Transformed data schema:")
    df_transformed.printSchema()
    df_transformed.show()

    # --- DATA QUALITY CHECKS ---
    print("Performing data quality checks...")

    # Check for nulls in the primary key column
    null_count = df_transformed.filter(col("product_id").isNull()).count()
    if null_count > 0:
        print(f"Data Quality Check FAILED: Found {null_count} nulls in product_id.")
        raise ValueError("Data quality check failed: product_id contains null values.")

    print("Data Quality Check PASSED: No nulls in product_id.")

    # --- LOAD ---
    db_url = "jdbc:postgresql://postgres-dw:5432/dw_database"
    db_properties = {
        "user": "dw_user",
        "password": "dw_password",
        "driver": "org.postgresql.Driver"
    }
    table_name = "dim_product"

    # Write the DataFrame to the new PostgreSQL table
    df_transformed.write.jdbc(
        url=db_url,
        table=table_name,
        mode="overwrite",
        properties=db_properties
    )

    print(f"Successfully loaded data into '{table_name}' table.")
    spark.stop()


if __name__ == "__main__":
    main()
