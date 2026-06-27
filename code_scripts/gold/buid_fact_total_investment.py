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

def build_fact_investment():
    dim_time = spark.table('gold.dim_time')
    dim_capital_source = spark.table('gold.dim_capital_source')
    
    total_investment = spark.table('silver.investment')
    

    result = (
        total_investment.alias("i")
        .join(
            dim_time.alias("t"),
            (col("i.year") == col("t.year")) &
            (col("i.quarter") == col("t.quarter")) &
            (col('t.month').isNull()),
            "left"
        )
        .join(
            dim_capital_source.alias("c"),
            col("i.investment_name") == col("c.source_name"),
            "left"
        )
        .select(
            col("i.*"),
            col("t.time_key"),
            col("c.capital_source_key")
        )
    )
    # Lấy giá trị quý trước
    result = (
        result.alias("c")
        .join(
            result.alias("p"),
            (
                (col("c.capital_source_key") == col("p.capital_source_key")) &
                (
                    (
                        (col("c.year") == col("p.year")) &
                        (col("c.quarter") == col("p.quarter") + 1)
                    ) |
                    (
                        (col("c.year") == col("p.year") + 1) &
                        (col("c.quarter") == 1) &
                        (col("p.quarter") == 4)
                    )
                )
            ),
            "left"
        )
        .select(
            col("c.*"),
            col("p.value").alias("investment_value_pre_quarter")
        )
    )

    # Lấy giá trị cùng quý năm trước
    result = (
        result.alias("c")
        .join(
            result.alias("p"),
            (col("c.capital_source_key") == col("p.capital_source_key")) &
            (col("c.year") == col("p.year") + 1) &
            (col("c.quarter") == col("p.quarter")),
            "left"
        )
        .select(
            col("c.*"),
            col("p.value").alias("investment_value_pre_year")
        )
    )

    # QoQ Growth
    result = result.withColumn(
        "qoq_growth_rate",
        when(
            col("investment_value_pre_quarter") > 0,
            round(
                (col("value") - col("investment_value_pre_quarter"))
                / col("investment_value_pre_quarter") * 100,
                3
            )
        )
    )

    # YoY Growth
    result = result.withColumn(
        "yoy_growth_rate",
        when(
            col("investment_value_pre_year") > 0,
            round(
                (col("value") - col("investment_value_pre_year"))
                / col("investment_value_pre_year") * 100,
                3
            )
        )
    )

    # Tỷ trọng nguồn vốn trong từng quý
    w = Window.partitionBy("year", "quarter")

    result = (
        result
        .withColumn(
            "total_value",
            sum("value").over(w)
        )
        .withColumn(
            "source_share_pct",
            round(
                col("value") / col("total_value") * 100,
                3
            )
        )
        .drop("total_value")
    )

    # Chọn đúng schema của fact
    result = (
        result.select(
            col("time_key").cast("int").alias("time_key"),
            col("capital_source_key").cast("int").alias("capital_source_key"),
            col("unit").cast("string").alias("unit"),
            col("value").cast("float").alias("investment_value"),
            col("investment_value_pre_quarter").cast("float"),
            col("investment_value_pre_year").cast("float"),
            col("qoq_growth_rate").cast("float"),
            col("yoy_growth_rate").cast("float"),
            col("source_share_pct").cast("float")
        )
        .fillna(
            0,
            subset=[
                "investment_value",
                "investment_value_pre_quarter",
                "investment_value_pre_year",
                "qoq_growth_rate",
                "yoy_growth_rate",
                "source_share_pct"
            ]
        )
    )
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_social_total_investment')
                
def main_build_fact_total_investment():
    print("BẮT ĐẦU LOAD DỮ LIỆU VÀO FACT TOTAL INVESTMENT")
    build_fact_investment()
    print("LOAD DỮ LIỆU FACT TOTAL INVESTMENT HOÀN THÀNH")
    spark.stop()
