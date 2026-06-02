from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql.types import *
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


def insert_df_to_table_silver_layer(df: pd.DataFrame, table_name, year=None, quarter=None):

    print('Bắt đầu insert dữ liệu vào SILVER layer')

    try:

        # ===== GDP xử lý riêng =====
        if table_name == 'gdp':

            df = df[
                [
                    'sector',
                    'sub_sector',
                    'year',
                    'quarter',
                    'value',
                    'type',
                    'unit',
                    'ingest_at'
                ]
            ].copy()

            df['year'] = pd.to_numeric(df['year'])
            df['quarter'] = pd.to_numeric(df['quarter'])
            df['value'] = pd.to_numeric(df['value'])

            spark_df = spark.createDataFrame(df)

            spark_df.show()

            # code quý 4 trước 2018
            if year < 2018 and quarter == 4:

                spark.sql(f"""
                    SELECT
                        sub_sector,
                        SUM(value) AS sum_123
                    FROM silver.gdp
                    WHERE year={year}
                    AND quarter < 4
                    GROUP BY sub_sector
                """).createOrReplaceTempView("pre_table")

                spark_df.createOrReplaceTempView("cur_table")

                spark_df = spark.sql("""
                    SELECT
                        c.sector,
                        c.sub_sector,
                        c.year,
                        c.quarter,
                        c.value - p.sum_123 AS value,
                        c.type,
                        c.unit,
                        c.ingest_at
                    FROM cur_table c
                    JOIN pre_table p
                    USING(sub_sector)
                """)

        else:

            # ===== tạo Spark DF cho các table khác =====
            spark_df = spark.createDataFrame(df)

            if 'year' in spark_df.columns:
                spark_df = spark_df.withColumn(
                    "year",
                    col("year").cast("int")
                )

            if 'quarter' in spark_df.columns:
                spark_df = spark_df.withColumn(
                    "quarter",
                    col("quarter").cast("int")
                )

        # =========================
        # TABLE-SPECIFIC LOGIC
        # =========================

        if table_name == 'investment':

            spark_df = spark_df.select(
                'investment_name','value','unit',
                'quarter','year','ingest_at'
            )

        elif table_name == 'international_ecommerce':

            spark_df = (
                spark_df
                .select(
                    'type','product_name','value',
                    'unit','quantity','month',
                    'quarter','year','ingest_at'
                )
                .withColumn("month", col("month").cast("int"))
                .withColumn("value", col("value").cast("double"))
                .withColumn("quantity", col("quantity").cast("double"))
            )

        elif table_name == 'forestry':

            spark_df = spark_df.select(
                'forestry_indicator','value',
                'unit','quarter','year','ingest_at'
            )

        elif table_name == 'livestock':

            spark_df = spark_df.select(
                'livestock_indicator','value',
                'unit','quarter','year','ingest_at'
            )

        elif table_name == 'aquatic_products':

            spark_df = spark_df.select(
                'aquatic_type','product_name',
                'value','unit',
                'quarter','year','ingest_at'
            )

        elif table_name == 'industry_product':

            spark_df = (
                spark_df
                .select(
                    'product_name','value','unit',
                    'month','quarter','year','ingest_at'
                )
                .withColumn("product_name", col("product_name").cast("string"))
                .withColumn("value", col("value").cast("double"))
                .withColumn("month", col("month").cast("int"))
            )

        elif table_name == 'annual_crops':

            spark_df = spark_df.select(
                'crop_name',
                'production','production_unit',
                'area','area_unit',
                'yield','yield_unit',
                'year','ingest_at'
            )

        elif table_name == 'staple_crops':

            spark_df = spark_df.select(
                'crop_name',
                'production','production_unit',
                'area','area_unit',
                'yield','yield_unit',
                'year','ingest_at'
            )

        elif table_name == 'perennial_crops':

            spark_df = spark_df.select(
                'crop_name',
                'production','production_unit',
                'yield','yield_unit',
                'area','area_unit',
                'year','ingest_at'
            )

        spark_df.printSchema()
        spark_df.show()

        # ===== WRITE =====

        spark_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema","true") \
            .saveAsTable(f"silver.{table_name}")

        print(
            f"Tải dữ liệu vào table {table_name} hoàn tất !!!!! {year} {quarter}"
        )

    except Exception as e:

        print(
            f'AN ERROR OCCURED WHEN LOAD DF TO '
            f'{table_name} - {year} {quarter} !!!!!\n{e}'
        )
    
