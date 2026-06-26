from pyspark.sql import SparkSession
import os

builder = SparkSession.builder \
    .appName("Delta-MinIO-Gold") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .config(
        "spark.sql.catalogImplementation",
        "hive"
    ) \
    .config(
        "hive.metastore.uris",
        "thrift://hive:9083"
    ) \
    .config(
        "spark.sql.warehouse.dir",
        "/tmp/spark-warehouse"
    ) \
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    ) \
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    ) \
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin"
    ) \
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    ) \
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .enableHiveSupport()

spark = builder.getOrCreate()

tables = [
    # "dim_time",
    # "dim_sector",
    # "dim_sub_sector",
    # "dim_product",
    # "dim_crop",
    # "dim_capital_source",
    # "fact_gdp_growth",
    # "fact_investment_by_sector",
    # "fact_crop_yield",
    # "fact_production_output",
    # "fact_international_trade",
    # "fact_social_total_investment"
    
    "staple_crops",
    'annual_crops',
    'perennial_crops',
    'gdp',
    'investment',
    'international_ecommerce',
    'forestry',
    'livestock',
    'aquatic_products',
    'industry_product',
    'investment_by_sector'
    
    
    
]

output_base = "/opt/spark/apps/tmp/silver_data"

for table_name in tables:

    print(f"Exporting gold.{table_name}")

    df = spark.sql(f"""
        SELECT *
        FROM silver.{table_name}
    """)

    output_path = os.path.join(
        output_base,
        table_name
    )

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"Done: {output_path}")

print("EXPORT COMPLETED")