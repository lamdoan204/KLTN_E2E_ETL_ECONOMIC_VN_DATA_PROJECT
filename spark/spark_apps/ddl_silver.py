from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# builder = SparkSession.builder \
#     .appName("Delta-MinIO") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.sql.catalogImplementation", "hive") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder = SparkSession.builder \
    .appName("Delta-MinIO") \
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

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql('DROP TABLE IF EXISTS silver.gdp;')
spark.sql("""
CREATE TABLE silver.gdp (
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
spark.sql('DROP TABLE IF EXISTS silver.investment;')

spark.sql("""
CREATE TABLE silver.investment (
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

spark.sql('DROP TABLE IF EXISTS silver.international_ecommerce;')
spark.sql("""
CREATE TABLE silver.international_ecommerce (
    product_name STRING,
    type STRING,
    value DOUBLE,
    unit STRING,
    quantity INT,
    quantity_unit STRING,
    month INT,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/international_ecommerce'
""")

spark.sql('DROP TABLE IF EXISTS silver.forestry;')

spark.sql("""
CREATE TABLE silver.forestry (
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
spark.sql('DROP TABLE IF EXISTS silver.livestock;')

spark.sql("""
CREATE TABLE silver.livestock (
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

spark.sql('DROP TABLE IF EXISTS silver.aquatic_products;')

spark.sql("""
CREATE TABLE silver.aquatic_products (
    aquatic_type STRING,
    product_name STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/aquatic_products'
""")
spark.sql('DROP TABLE IF EXISTS silver.industry_product;')

spark.sql("""
CREATE TABLE silver.industry_product (
    product_name STRING,
    value DOUBLE,
    unit STRING,
    month INT,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/industry_product'
""")
spark.sql('DROP TABLE IF EXISTS silver.investment_by_sector;')

spark.sql("""
CREATE TABLE silver.investment_by_sector (
    name STRING,
    value DOUBLE,
    unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/investment_by_sector'
""")
spark.sql('DROP TABLE IF EXISTS silver.annual_crops;')

spark.sql("""
CREATE TABLE silver.annual_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
    area DOUBLE,
    area_unit STRING,
    yield DOUBLE,
    yield_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/annual_crops'
""")
spark.sql('DROP TABLE IF EXISTS silver.staple_crops;')

spark.sql("""
CREATE TABLE silver.staple_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
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
spark.sql('DROP TABLE IF EXISTS silver.perennial_crops;')

spark.sql("""
CREATE TABLE silver.perennial_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
    yield DOUBLE,
    yield_unit STRING,
    area DOUBLE,
    area_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/perennial_crops'
""")