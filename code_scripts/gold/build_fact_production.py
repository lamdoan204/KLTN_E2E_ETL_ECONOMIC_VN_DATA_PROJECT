from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, round, lit, concat_ws
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

def build_fact_production_output():
    
    industry_product = spark.table('silver.industry_product')
    livestock = spark.table('silver.livestock')
    aquatic = spark.table('silver.aquatic_products')
    forestry = spark.table('silver.forestry')
    
    dim_time = spark.table('gold.dim_time')
    dim_product = spark.table('gold.dim_product')

    industry = (
        industry_product
        .select(
            lit("Industry Product").alias("product_category"),
            lit('not available').alias('product_type'), 
            col("product_name"),
            col("value"),
            col("unit"),
            col("month"),
            col("quarter"),
            col("year"),
        )
    )
    livestock = (
        livestock
        .select(
            lit("Livestock Product").alias("product_category"),
            lit('not available').alias('product_type'),
            col("livestock_indicator").alias("product_name"),
            col("value"),
            col("unit"),
            lit(None).cast("int").alias("month"),
            col("quarter"),
            col("year"),
        )
    )
    forestry = (
        forestry
        .select(
            lit("Forestry Product").alias("product_category"),
            lit("not available").alias('product_type'),
            col("forestry_indicator").alias("product_name"),
            col("value"),
            col("unit"),
            lit(None).cast("int").alias("month"),
            col("quarter"),
            col("year"),
        )
    )

    aquatic = (
        aquatic
        .select(
            lit("Aquatic Product").alias("product_category"),
            col('aquatic_type').alias('product_type'),
            col("product_name"),
            col("value"),
            col("unit"),
            lit(None).cast("int").alias("month"),
            col("quarter"),
            col("year"),
        )
    )
    product = (
        industry
        .unionByName(livestock)
        .unionByName(forestry)
        .unionByName(aquatic)
    )
    
    
    product = (
        product.alias('p')
        .join(
            dim_time.alias('t'),
            (col('p.year') == col('t.year')) &
            (col('p.quarter') == col('t.quarter')) & 
            (col('p.month').isNull()) |
            (col('p.year') == col('t.year')) &
            (col('p.month') == col('t.month')),
            "left"
        ).select(
        "p.*",
        "t.time_key"
        )
    )
    
    product = (
        product.alias('p')
        .join(
            dim_product.alias('t'),
            (col('p.product_category') == col('t.product_category')) &
            (col('p.product_name') == col('t.product_name')) &
            (col('p.product_type') == col('t.product_type')),
            "left"
        )
    )
    cur = product.alias("c")
    pre_q = product.alias("pq")
    pre_y = product.alias("py")
    
    result = (
        cur
        .join(
            pre_q,
            (col("c.product_key") == col("pq.product_key")) &
            (
                (
                    (col("c.year") == col("pq.year")) &
                    (col("c.quarter") == col("pq.quarter") + 1)
                ) |
                (
                    (col("c.year") == col("pq.year") + 1) &
                    (col("c.quarter") == 1) &
                    (col("pq.quarter") == 4)
                )
            ),
            "left"
        )
        .join(
            pre_y,
            (col("c.product_key") == col("py.product_key")) &
            (col("c.year") == col("py.year") + 1) &
            (col("c.quarter") == col("py.quarter")),
            "left"
        )
        .select(
            col("c.*"),
            col("pq.value").alias("prev_quarter_value"),
            col("py.value").alias("pre_year_value")
        )
    )
    result = (
        result
        .withColumn(
            "qoq_growth_rate",
            when(
                col("prev_quarter_value") > 0,
                round(
                    (col("value") - col("prev_quarter_value"))
                    / col("prev_quarter_value") * 100,
                    3
                )
            )
        )
        .withColumn(
            "yoy_growth_rate",
            when(
                col("pre_year_value") > 0,
                round(
                    (col("value") - col("pre_year_value"))
                    / col("pre_year_value") * 100,
                    3
                )
            )
        )
    )
    
    w_year = Window.partitionBy("year", 'quarter')

    result = (
        result
        .withColumn(
            "total_value",
            sum("value").over(w_year)
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

            col("value").cast("float").alias("value"),
            col("unit").cast("string").alias("unit"),

            col("prev_quarter_value").cast("float").alias("prev_quarter_value"),
            col("pre_year_value").cast("float").alias("pre_year_value"),

            col("yoy_growth_rate").cast("float").alias("yoy_growth_rate"),
            col("qoq_growth_rate").cast("float").alias("qoq_growth_rate"),

            col("product_share_pct").cast("float").alias("product_share_pct")
        )
        .fillna(
            0,
            subset=[
                "value",
                "prev_quarter_value",
                "pre_year_value",
                "yoy_growth_rate",
                "qoq_growth_rate",
                "product_share_pct"
            ]
        )
    )
   
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_production_output')
    result.show(100)
    

    
def main_build_fact_production_output():
    build_fact_production_output()
