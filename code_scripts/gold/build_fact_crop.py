from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, round
from pyspark.sql.functions import first
from pyspark.sql.functions import sum
import gc
# ════════════════════════════════════════════════════════
# 0. SPARK SESSION
# ════════════════════════════════════════════════════════
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

def build_fact_crop():
    perennial = spark.table('silver.perennial_crops')
    staple = spark.table('silver.staple_crops')
    annual = spark.table('silver.annual_crops')
    
    dim_time = spark.table('gold.dim_time')
    dim_crop = spark.table('gold.dim_crop')
    crop = (
        perennial
        .unionByName(staple)
        .unionByName(annual)
    )
    
    crop = (
        crop.alias("c")
        .join(
            dim_time.alias("t"),
            (col("c.year") == col("t.year")) &
            (col("t.month").isNull()) &
            (col("t.quarter").isNull()) &
            (col("t.day").isNull()),
            "left"
        )
    )
    crop = (
        crop.alias("c")
        .join(
            dim_crop.alias("t"),
            (col('c.crop_name') == col('t.crop_name')),
            "left"
        )
    )
    del dim_crop
    del dim_time
    cur = result.alias("c")
    pre = result.alias("p")

    result = (
        cur.join(
            pre,
            (col("c.crop_key") == col("p.crop_key")) &
            (col("c.year") == col("p.year") + 1),
            "left"
        )
        .select(
            col("c.*"),

            col("p.area").alias("area_pre_year"),
            col("p.yield").alias("yield_pre_year"),
            col("p.productivity").alias("productivity_pre_year")
        )
    )
    del cur
    del pre
    result = result.withColumn(
    "yield_yoy_growth_rate",
        when(
            col("yield_pre_year") > 0,
            round(
                (col("yield") - col("yield_pre_year"))
                / col("yield_pre_year") * 100,
                3
            )
        )
    )
    w_year = Window.partitionBy("year")

    result = (
        result
        .withColumn(
            "total_yield",
            sum("yield").over(w_year)
        )
        .withColumn(
            "yield_share_pct",
            round(
                col("yield") / col("total_yield") * 100,
                3
            )
        )
        .drop("total_yield")
    )
    result = result.withColumn('production', round(col('production'), 3))
    result = (
        result.select(
            col("time_key").cast("int").alias("time_key"),
            col("crop_key").cast("int").alias("crop_key"),

            col("production_unit").cast("string").alias("production_unit"),
            col("yield_unit").cast("string").alias("yield_unit"),
            col("area_unit").cast("string").alias("area_unit"),

            col("area").cast("float").alias("area"),
            col("yield").cast("float").alias("yield_value"),
            col("productivity").cast("float").alias("productivity"),

            col("area_pre_year").cast("float").alias("area_pre_year"),
            col("yield_pre_year").cast("float").alias("yield_pre_year"),
            col("productivity_pre_year").cast("float").alias("productivity_pre_year"),

            col("yield_yoy_growth_rate").cast("float").alias("yield_yoy_growth_rate"),
            col("yield_share_pct").cast("float").alias("yield_share_pct")
        )
        .fillna(0)
    )
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_crop_yield')
    result.show(100)
    
    del result
    
        
def main_build_fact_crop():
    build_fact_crop()
    spark.stop()