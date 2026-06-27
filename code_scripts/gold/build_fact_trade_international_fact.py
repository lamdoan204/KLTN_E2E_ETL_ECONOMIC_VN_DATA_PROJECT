from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, round
from pyspark.sql.functions import first
from pyspark.sql.functions import sum
import gc

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

def build_fact_trade_international():
    dim_time = spark.table('gold.dim_time')
    dim_product = spark.table('gold.dim_product')

    trade_product = spark.table('silver.international_ecommerce')
    trade_product.show(100)
    
    result = (
        trade_product.alias('p')
        .join(
            dim_product.alias('dp'),
            (col('p.product_name') == col('dp.product_name'))&
            (col('p.type') == col('dp.product_type')),
            "left"
        )
    ).select(
        col('p.*'),
        col('dp.product_key')
    )
    result = (
        result.alias('p')
        .join(
            dim_time.alias('t'),
            (col('p.year') == col('t.year')) &
            (col('p.quarter') == col('t.quarter')) & 
            (col('p.month') == col('t.month')),
            "left"
        ).select(
            col('p.*'),
            col('t.time_key')
        )
    )
    cur = result
    pre = result
    
    a = (
        cur.alias('c')
        .join(
            pre.alias('p'),
            (col('c.year') == col('p.year') + 1) &
            (col('c.month') == col('p.month')) & 
            (col("c.product_key") == col('p.product_key')),
            'left'
        ).select(
            col('c.*'),
            col('p.value').alias('trade_value_pre_year')
        )
    )
    
    a = (
         a.alias('c')
        .join(
            pre.alias("p"),
            (
                (
                    (col("c.year") == col("p.year")) &
                    (col("c.month") == col("p.month") + 1)
                ) |
                (
                    (col("c.year") == col("p.year") + 1) &
                    (col("c.month") == 1) &
                    (col("p.month") == 12)
                )
            ) &
            (col("c.product_key") == col("p.product_key")),
            "left"
        ).select(
            col('c.*'),
            col('p.value').alias('trade_value_pre_month')
        )
    )
    result = a.withColumn(
        "mom_growth_rate",
        when(
            col("trade_value_pre_month") > 0,
            round(
                (col("value") - col("trade_value_pre_month"))
                / col("trade_value_pre_month") * 100,
                3
            )
        )
    )

    # YoY Growth Rate
    result = result.withColumn(
        "yoy_growth_rate",
        when(
            col("trade_value_pre_year") > 0,
            round(
                (col("value") - col("trade_value_pre_year"))
                / col("trade_value_pre_year") * 100,
                3
            )
        )
    )
    w = Window.partitionBy("year", 'quarter', 'month')

    result = (
        result
        .withColumn(
            "total_value",
            sum("value").over(w)
        )
        .withColumn(
            "product_share_pct",
            round(
                col("value") / col("total_value") * 100,
                3
            )
        )
        .drop("total_value")
    )
    result = (
        result.select(
            col("time_key").cast("int").alias("time_key"),
            col("product_key").cast("int").alias("product_key"),

            col("value").cast("float").alias("trade_value"),
            col("unit").cast("string").alias("value_unit"),

            col("quantity").cast("float").alias("quantity"),
            col("quantity_unit").cast("string").alias("quantity_unit"),

            col("trade_value_pre_month").cast("float").alias("trade_value_pre_month"),
            col("trade_value_pre_year").cast("float").alias("trade_value_pre_year"),

            col("mom_growth_rate").cast("float").alias("mom_growth_rate"),
            col("yoy_growth_rate").cast("float").alias("yoy_growth_rate"),

            col("product_share_pct").cast("float").alias("product_share_pct")
        )
        .fillna(
            0,
            subset=[
                "trade_value",
                "quantity",
                "trade_value_pre_month",
                "trade_value_pre_year",
                "mom_growth_rate",
                "yoy_growth_rate",
                "product_share_pct"
            ]
        )
    )
    result.show(100)    
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_international_trade')
    del result
    del cur
    del pre
    del a
    del dim_product
    del dim_time
    del trade_product

def main_build_fact_trade_international():
    print("BẮT ĐẦU LOAD DATA VÀO FACT TRADE INTERNATIONAL =======================")
    build_fact_trade_international()
    print("LOAD DATA VÀO FACT TRADE INTERNATIONAL HOÀN THÀNH =======================")
