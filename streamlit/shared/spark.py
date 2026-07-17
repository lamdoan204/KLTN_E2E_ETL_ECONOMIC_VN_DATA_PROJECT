# -*- coding: utf-8 -*-
"""
shared/spark.py
================
Kết nối Spark dùng chung cho toàn bộ app — mọi tab/fact đều gọi qua đây.
"""

import os
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

APP_NAME = os.getenv("APP_NAME", "Economic Dashboard")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")


@st.cache_resource(show_spinner="Đang kết nối Spark…")
def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "1"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


@st.cache_data(ttl=300, show_spinner=False)
def qry(_spark, sql: str) -> pd.DataFrame:
    """Chạy SQL trên Spark và trả về pandas DataFrame, cache 5 phút."""
    return _spark.sql(sql).toPandas()