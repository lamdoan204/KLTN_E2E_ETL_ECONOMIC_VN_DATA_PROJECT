from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
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


def insert_df_to_table_silver_layer(df: pd.DataFrame, table_name):
    try:
        spark_df = spark.createDataFrame(df)
        df = None
        if table_name == 'gdp':
            df = spark_df.select('sector', 'sub_sector', 'year', 'quarter', 'value', 'type', 'unit', 'ingest_at')
            # code quý 4 trước 2018: tính lại từ tổng quý 1 2 3 
            
                
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"silver.{table_name}")
        

    except Exception as e:
        print(f'AN ERROR OCCURED WHEN LOAD DF TO {table_name} !!!!!!!!!!!!!! \n {e}')
    
    next
