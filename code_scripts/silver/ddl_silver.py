from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

import boto3
from botocore.client import Config


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




s3_client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

def delete_minio_prefix(bucket: str, prefix: str):
    """
    Xoá toàn bộ object trong MinIO dưới một prefix cụ thể.
 
    Cần thiết vì sau khi VACUUM và DROP TABLE (external table), Delta vẫn
    còn giữ lại _delta_log/ và các file Parquet active hiện tại trên MinIO.
    Boto3 xoá vật lý hoàn toàn, đảm bảo CREATE TABLE tiếp theo tạo bảng
    thực sự trắng tinh.
 
    Args:
        bucket: tên bucket MinIO (ví dụ: 'silver')
        prefix: prefix thư mục cần xoá (ví dụ: 'gdp/')
    """
    prefix = prefix.rstrip("/") + "/"  # đảm bảo có trailing slash
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
 
    deleted_count = 0
    for page in pages:
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3_client.delete_objects(Bucket=bucket, Delete=delete_payload)
        deleted_count += len(objects)
 
    print(f"  → Đã xoá {deleted_count} object(s) tại s3://{bucket}/{prefix}")
 



spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql('DROP TABLE IF EXISTS silver.gdp;')

delete_minio_prefix('silver', 'gdp')

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
delete_minio_prefix('silver', 'investment')

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
delete_minio_prefix('silver', 'international_ecommerce')

spark.sql("""
CREATE TABLE silver.international_ecommerce (
    product_name STRING,
    type STRING,
    value DOUBLE,
    unit STRING,
    quantity DOUBLE,
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
delete_minio_prefix('silver', 'forestry')

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
delete_minio_prefix('silver', 'livestock')

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
delete_minio_prefix('silver', 'aquatic_products')

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
delete_minio_prefix('silver', 'industry_product')

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
delete_minio_prefix('silver', 'investment_by_sector')

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
delete_minio_prefix('silver', 'annual_crops')

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
delete_minio_prefix('silver', 'staple_crops')

spark.sql("""
CREATE TABLE silver.staple_crops (
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
LOCATION 's3a://silver/staple_crops'
""")
spark.sql('DROP TABLE IF EXISTS silver.perennial_crops;')
delete_minio_prefix('silver', 'perennial_crops')

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