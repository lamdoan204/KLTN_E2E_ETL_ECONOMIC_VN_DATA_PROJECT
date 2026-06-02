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
            print("Đang tải dữ liệu vào Silver.GDP")
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
        elif table_name == 'investment':
            print("Đang tải dữ liệu vào Silver.INVESTMENT")
            df = spark_df.select('investment_name', 'value', 'unit', 'quarter', 'year',  'ingest_at') # sắp xếp lại columns
            df.show()
            
        elif table_name == 'international_ecommerce':
            print('Đang tải dữ liệu vào Silver.International_ecomerce !!!')
            df = spark_df.select('type', 'product_name', 'value', 'unit', 'quantity', 'quarter', 'year', 'ingest_at')
            df.show()
        elif table_name == 'forestry':
            print('Đang tải dữ liệu vào Silver.Forestry !!!!!')
            df = spark_df.select('forestry_indicator', 'value', 'unit', 'quarter', 'year', 'ingest_at')
            df.show()
        elif table_name == 'livestock':
            print('Đang tải dữ liệu vào SILVER.Livestock !!!!!')
            df = spark_df.select('livestock_indicator', 'value', 'unit', 'quarter', 'year', 'ingest_at')
            df.show()
            
        elif table_name == 'aquatic_products':
            print("Đang tải dữ liệu vào SILVER.Aquatic_products")
            df = spark_df.select('aquatic_type', 'product_name', 'value', 'unit', 'quarter', 'year', 'ingest_at')
            df.show()
        elif table_name == 'industry_product':
            print("Đang tải dữ liệu vào SILVER.Industry_product !!!!!")
            df = spark_df.select('product_name', 'value', 'unit', 'month', 'quarter' 'year', 'ingest_at')
            df.show()
        elif table_name == 'investment_by_sector':
            next
        elif table_name == 'annual_crops':
            print("Đang tải dữ liệu vào SILVER.annual_crops !!!!!")
            df = spark_df.select('crop_name', 'production', 'production_unit', 'area', 'area_unit', 'yield', 'yield_unit', 'year', 'ingest_at')
            df.show()
            
        elif table_name == 'staple_crops':
            print("Đang tải dữ liệu vào SILVER.Staple_crops !!!!!")
            df = spark_df.select('crop_name', 'production', 'production_unit', 'area', 'area_unit', 'yield', 'yield_unit', 'year', 'ingest_at')
            df.show()
        else: 
            # cây lâu năm
            print('Đang tải dữ liệu vào SILVER.Perennial_Crops')
            df = spark_df.select('crop_name',  'production',  'production_unit', 'yield', 'yield_unit', 'area', 'area_unit', 'year', 'ingest_at')
            df.show()

        

        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"silver.{table_name}")
        print(f"Tải dữ liệu vào table: {table_name} hoàn tất !!!!!!!!")

    
        

    except Exception as e:
        print(f'AN ERROR OCCURED WHEN LOAD DF TO {table_name} !!!!!!!!!!!!!! \n {e}')
    
    next
