from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

from pyspark.sql.functions import col

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
        "s3a://warehouse/"
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


def insert_df_to_table_silver_layer(df: pd.DataFrame, table_name, year = None):
    print('Bắt đầu insert dữ liệu vào SILVER layer')
    try:
        
        spark_df = spark.createDataFrame(df)
        spark_df = spark_df.withColumn(
            "year",
            col("year").cast("int")
        ).withColumn(
            "quarter",
            col("quarter").cast("int")
)
        spark_df.printSchema()
        if table_name == 'gdp':
            df = spark_df.select('sector', 'sub_sector', 'year', 'quarter', 'value', 'type', 'unit', 'ingest_at') # sắp xếp lại columns
            df.show()
            
            # code quý 4 trước 2018: tính lại từ tổng quý 1 2 3
            if year is not None:
                spark.sql(
                    f""" 
                    select
                    sub_sector,
                    sum(value) as sum_123

                    from silver.gdp
                    where year = {year} and quarter < 4
                    group by sub_sector
                    """).createOrReplaceTempView("pre_table")
                
                df.createOrReplaceTempView('cur_table')
                df  = spark.sql("""
                                select 
                                c.sector,
                                c.sub_sector,
                                c.year,
                                c.quarter,
                                c.value - sum_123 as value,
                                c.type,
                                c.unit,
                                c.ingest_at
                                from cur_table as c 
                                join pre_table as p using(sub_sector) 
                                """)
                # c.sub_sector = p.sub_sector
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"silver.{table_name}")
        

    except Exception as e:
        print(f'AN ERROR OCCURED WHEN LOAD DF TO {table_name} !!!!!!!!!!!!!! \n {e}')
    
    next
