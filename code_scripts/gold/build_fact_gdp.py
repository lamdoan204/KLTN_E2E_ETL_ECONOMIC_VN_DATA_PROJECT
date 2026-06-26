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

def build_fact_gdp_growth():
    gdp = spark.table('silver.gdp')
    dim_time = spark.table('gold.dim_time')
    dim_sub = spark.table('gold.dim_sub_sector')
    
    gdp_fact = (
        gdp.alias('g')
        .join(
            dim_time.alias('t'),
            (col('g.year') == col('t.year')) &
            (col('g.quarter') == col('t.quarter')) &
            (col('t.month').isNull()) & (col('t.day').isNull()),
            "left"     
        )
        .select(
            (col('t.time_key')),
            col('g.*')
        )
    )
   
    gdp_fact = (
        gdp_fact.alias('g')
        .join(
            dim_sub.alias('s'),
            (col('g.sub_sector') == col('s.sub_sector_name')),
            "left"
        )
        .select(
            (col('s.sub_sector_key')),
            col('g.*')
        )
    )
    
    gdp_pivot = (
        gdp_fact
        .groupBy("sub_sector_key", "time_key", "unit", 'year', 'quarter')
        .pivot("type")
        .agg(first("value"))
    )
    
    gdp_pivot = (
        gdp_pivot
        .withColumnRenamed("Giá trị hiện hành", "market_value")
        .withColumnRenamed("Giá trị so sánh", "constant_value")
    )
    
    del dim_time
    gc.collect()
    
    current = gdp_pivot.alias("c")
    prev_q = gdp_pivot.alias("pq")
    prev_y = gdp_pivot.alias("py")

    del gdp_pivot
    gc.collect()

    result = (
        current
        # Quý trước
        .join(
            prev_q,
            (col("c.sub_sector_key") == col("pq.sub_sector_key")) &
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
        # Cùng quý năm trước
        .join(
            prev_y,
            (col("c.sub_sector_key") == col("py.sub_sector_key")) &
            (col("c.year") == col("py.year") + 1) &
            (col("c.quarter") == col("py.quarter")),
            "left"
        )
        .select(
            col("c.*"),

            col("pq.market_value").alias("market_value_pre_quarter"),
            col("pq.constant_value").alias("constant_value_pre_quarter"),

            col("py.market_value").alias("market_value_pre_year"),
            col("py.constant_value").alias("constant_value_pre_year"),
        )
    )    
    del current
    del prev_q
    del prev_y
    result = (
        result
        .withColumn(
            "market_qoq_growth_rate",
            when(
                col("market_value_pre_quarter").isNotNull(),
                round((col("market_value") - col("market_value_pre_quarter"))
                / col("market_value_pre_quarter") * 100, 3)
            )
        )
        .withColumn(
            "market_yoy_growth_rate",
            when(
                col("market_value_pre_year").isNotNull(),
                round((col("market_value") - col("market_value_pre_year"))
                / col("market_value_pre_year") * 100, 3)
            )
        )
        .withColumn(
            "real_qoq_growth_rate",
            when(
                col("constant_value_pre_quarter").isNotNull(),
                round((col("constant_value") - col("constant_value_pre_quarter"))
                / col("constant_value_pre_quarter") * 100, 3)
            )
        )
        .withColumn(
            "real_yoy_growth_rate",
            when(
                col("constant_value_pre_year").isNotNull(),
                round((col("constant_value") - col("constant_value_pre_year"))
                / col("constant_value_pre_year") * 100, 3)
            )
        )
    )
    result = result.withColumn(
        "implicit_price_deflator",
        round(col("market_value") / col("constant_value") * 100, 3)
    )
    result = (
        result.join(
            dim_sub.select("sub_sector_key", "sector_key"),
            "sub_sector_key"
        )
    )
    w_sector = Window.partitionBy("sector_key", "time_key")

    result = (
        result
        .withColumn(
            "sector_total",
            sum("market_value").over(w_sector)
        )
        .withColumn(
            "sector_share_pct",
            round(col("market_value") / col("sector_total") * 100, 3)
        )
        .drop("sector_total")
    )
    w_year = Window.partitionBy("year", 'quarter')

    result = (
        result
        .withColumn(
            "gdp_total",
            sum("market_value").over(w_year)
        )
        .withColumn(
            "gdp_share_pct",
           round( col("market_value") / col("gdp_total") * 100, 3)
        )
        .drop("gdp_total")
    )
    del dim_sub
    result = (
        result
        .select(
            "time_key",
            "sub_sector_key",
            "unit",
            "market_value",
            "constant_value",
            "market_value_pre_quarter",
            "market_value_pre_year",
            "constant_value_pre_quarter",
            "constant_value_pre_year",
            "market_qoq_growth_rate",
            "market_yoy_growth_rate",
            "real_qoq_growth_rate",
            "real_yoy_growth_rate",
            "implicit_price_deflator",
            "sector_share_pct",
            "gdp_share_pct"
        )
        .fillna(0)
    )
    result = (
        result
        .withColumn("market_value", col("market_value").cast("float"))
        .withColumn("constant_value", col("constant_value").cast("float"))
        .withColumn("market_value_pre_quarter", col("market_value_pre_quarter").cast("float"))
        .withColumn("market_value_pre_year", col("market_value_pre_year").cast("float"))
        .withColumn("constant_value_pre_quarter", col("constant_value_pre_quarter").cast("float"))
        .withColumn("constant_value_pre_year", col("constant_value_pre_year").cast("float"))
        .withColumn("market_qoq_growth_rate", col("market_qoq_growth_rate").cast("float"))
        .withColumn("market_yoy_growth_rate", col("market_yoy_growth_rate").cast("float"))
        .withColumn("real_qoq_growth_rate", col("real_qoq_growth_rate").cast("float"))
        .withColumn("real_yoy_growth_rate", col("real_yoy_growth_rate").cast("float"))
        .withColumn("implicit_price_deflator", col("implicit_price_deflator").cast("float"))
        .withColumn("sector_share_pct", col("sector_share_pct").cast("float"))
        .withColumn("gdp_share_pct", col("gdp_share_pct").cast("float"))
    )
    
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_gdp_growth')
    
    del result    
    
def main_build_fact_gdp():
    build_fact_gdp_growth()
    spark.stop()