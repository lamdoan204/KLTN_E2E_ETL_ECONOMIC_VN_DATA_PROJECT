from pyspark.sql import SparkSession

import boto3
from botocore.client import Config

def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Delta-MinIO-Gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.delta.logStore.s3a.class", "org.apache.spark.sql.delta.storage.S3AStorageLogStore")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive:9083")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark

spark = get_spark()

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
 


spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# ─────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────

spark.sql("DROP TABLE IF EXISTS gold.dim_time;")
delete_minio_prefix('gold', 'dim_time')
spark.sql("""
CREATE TABLE gold.dim_time (
    time_key    INT,
    year        INT,
    quarter     INT,
    month       INT,
    day         INT
)
USING DELTA
LOCATION 's3a://gold/dim_time'
""")

spark.sql("DROP TABLE IF EXISTS gold.dim_sector;")
delete_minio_prefix('gold', 'dim_sector')

spark.sql("""
CREATE TABLE gold.dim_sector (
    sector_key  INT,
    sector_name STRING
)
USING DELTA
LOCATION 's3a://gold/dim_sector'
""")

spark.sql("DROP TABLE IF EXISTS gold.dim_sub_sector;")

delete_minio_prefix('gold', 'dim_sub_sector')


spark.sql("""
CREATE TABLE gold.dim_sub_sector (
    sub_sector_key  INT,
    sub_sector_name STRING,
    sector_key      INT
)
USING DELTA
LOCATION 's3a://gold/dim_sub_sector'
""")

spark.sql("DROP TABLE IF EXISTS gold.dim_product;")
delete_minio_prefix('gold', 'dim_product')

spark.sql("""
CREATE TABLE gold.dim_product (
    product_key      INT ,
    product_name     STRING,
    product_type     STRING,
    product_category STRING
)
USING DELTA
LOCATION 's3a://gold/dim_product'
""")

spark.sql("DROP TABLE IF EXISTS gold.dim_crop;")
delete_minio_prefix('gold', 'dim_crop')

spark.sql("""
CREATE TABLE gold.dim_crop (
    crop_key      INT,
    crop_name     STRING,
    crop_category STRING
)
USING DELTA
LOCATION 's3a://gold/dim_crop'
""")

spark.sql("DROP TABLE IF EXISTS gold.dim_capital_source;")
delete_minio_prefix('gold', 'dim_capital_source')

spark.sql("""
CREATE TABLE gold.dim_capital_source (
    capital_source_key  INT,
    source_name         STRING
)
USING DELTA
LOCATION 's3a://gold/dim_capital_source'
""")

# ─────────────────────────────────────────
# FACT TABLES
# ─────────────────────────────────────────

spark.sql("DROP TABLE IF EXISTS gold.fact_gdp_growth;")
delete_minio_prefix('gold', 'fact_gdp_growth')

spark.sql("""
CREATE TABLE gold.fact_gdp_growth (
    time_key                    INT,
    sub_sector_key              INT,
    unit                        STRING,
    market_value                FLOAT,
    constant_value              FLOAT,
    market_value_pre_quarter    FLOAT,
    market_value_pre_year       FLOAT,
    constant_value_pre_quarter  FLOAT,
    constant_value_pre_year     FLOAT,
    market_qoq_growth_rate      FLOAT,
    market_yoy_growth_rate      FLOAT,
    real_qoq_growth_rate        FLOAT,
    real_yoy_growth_rate        FLOAT,
    implicit_price_deflator     FLOAT,
    sector_share_pct            FLOAT,
    gdp_share_pct               FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_gdp_growth'
""")

spark.sql("DROP TABLE IF EXISTS gold.fact_investment_by_sector;")
delete_minio_prefix('gold', 'fact_investment_by_sector')

spark.sql("""
CREATE TABLE gold.fact_investment_by_sector (
    time_key                INT,
    sector_key              INT,
    sub_sector_key          INT,
    unit                    STRING,
    investment_value        FLOAT,
    investment_value_pre_year FLOAT,
    yoy_growth_rate         FLOAT,
    sector_share_pct        FLOAT,
    all_sector_share_pct    FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_investment_by_sector'
""")

spark.sql("DROP TABLE IF EXISTS gold.fact_crop_yield;")
delete_minio_prefix('gold', 'fact_crop_yield')

spark.sql("""
CREATE TABLE gold.fact_crop_yield (
    time_key                INT,
    crop_key                INT,
    production_unit         STRING,
    yield_unit              STRING,
    area_unit               STRING,
    area                    FLOAT,
    yield_value             FLOAT,
    productivity            FLOAT,
    area_pre_year           FLOAT,
    yield_pre_year          FLOAT,
    productivity_pre_year   FLOAT,
    yield_yoy_growth_rate   FLOAT,
    yield_share_pct         FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_crop_yield'
""")

spark.sql("DROP TABLE IF EXISTS gold.fact_production_output;")
delete_minio_prefix('gold', 'fact_production_output')

spark.sql("""
CREATE TABLE gold.fact_production_output (
    time_key            INT,
    product_key         INT,
    value               FLOAT,
    unit                STRING,
    prev_quarter_value  FLOAT,
    pre_year_value      FLOAT,
    yoy_growth_rate     FLOAT,
    qoq_growth_rate     FLOAT,
    product_share_pct   FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_production_output'
""")

spark.sql("DROP TABLE IF EXISTS gold.fact_international_trade;")
delete_minio_prefix('gold', 'fact_international_trade')

spark.sql("""
CREATE TABLE gold.fact_international_trade (
    time_key                INT,
    product_key             INT,
    trade_value             FLOAT,
    value_unit              STRING,
    quantity                FLOAT,
    quantity_unit           STRING,
    trade_value_pre_month   FLOAT,
    trade_value_pre_year    FLOAT,
    mom_growth_rate         FLOAT,
    yoy_growth_rate         FLOAT,
    product_share_pct       FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_international_trade'
""")

spark.sql("DROP TABLE IF EXISTS gold.fact_social_total_investment;")
delete_minio_prefix('gold', 'fact_social_total_investment')

spark.sql("""
CREATE TABLE gold.fact_social_total_investment (
    time_key                        INT,
    capital_source_key              INT,
    unit                            STRING,
    investment_value                FLOAT,
    investment_value_pre_quarter    FLOAT,
    investment_value_pre_year       FLOAT,
    qoq_growth_rate                 FLOAT,
    yoy_growth_rate                 FLOAT,
    source_share_pct                FLOAT
)
USING DELTA
LOCATION 's3a://gold/fact_social_total_investment'
""")