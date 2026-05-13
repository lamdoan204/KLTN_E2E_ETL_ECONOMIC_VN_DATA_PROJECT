from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

builder = SparkSession.builder \
    .appName("Delta-MinIO") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = builder.getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.gdp (
    sector STRING,
    sub_sector STRING,
    year INT,
    quarter INT,
    value DOUBLE,
    type STRING,
    unit STRING,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/gdp'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.investment (
    investment_name STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/investment'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.international_ecommerce (
    type STRING,
    product_name STRING,
    value DOUBLE,
    unit STRING,
    quantity INT,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/international_ecommerce'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.forestry (
    forestry_indicator STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/forestry'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.livestock (
    livestock_indicator STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/livestock'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.aquatic_products (
    aquatic_type STRING,
    production_type STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/aquatic_products'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.industry_product (
    product_name STRING,
    value DOUBLE,
    unit STRING,
    month INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/industry_product'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.investment_by_sector (
    name STRING,
    value DOUBLE,
    unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/investment_by_sector'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.annual_crops (
    crop_name STRING,
    area DOUBLE,
    area_unit STRING,
    crop_yield DOUBLE,
    crop_yield_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/annual_crops'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.staple_crops (
    crop_name STRING,
    type STRING,
    yield DOUBLE,
    yield_unit STRING,
    area DOUBLE,
    area_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/staple_crops'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.perennial_crops (
    crop_name STRING,
    crop_yield DOUBLE,
    yield_unit STRING,
    area DOUBLE,
    area_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/perennial_crops'
""")