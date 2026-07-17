from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def load_investment_by_sector(spark: SparkSession):
    # ---------- 1. Đọc nguồn ----------
    src = spark.table("silver.investment_by_sector").select("name", "value", "unit", "year")
    dim_sector = spark.table("gold.dim_sector")
    dim_sub_sector = spark.table("gold.dim_sub_sector")
    dim_time = spark.table("gold.dim_time").filter(F.col("quarter").isNull())

    # ---------- 2. Resolve sector_key / sub_sector_key ----------
    resolved = (
        src.alias("s")
        .join(dim_sub_sector.alias("dsub"), F.col("s.name") == F.col("dsub.sub_sector_name"), "left")
        .join(dim_sector.alias("dsec"), F.col("s.name") == F.col("dsec.sector_name"), "left")
        .select(
            F.col("s.year").alias("year"),
            F.col("s.unit").alias("unit"),
            F.col("s.value").alias("investment_value"),
            F.col("s.name").alias("name"),
            F.col("dsub.sub_sector_key").alias("sub_sector_key"),
            F.coalesce(F.col("dsub.sector_key"), F.col("dsec.sector_key")).alias("sector_key"),
        )
    )

    # ---------- 3. Window theo name (để lấy giá trị năm trước) ----------
    w_name_year = Window.partitionBy("name").orderBy("year")

    # ---------- 4. Window theo sector + year (tổng sub-sector trong 1 sector) ----------
    w_sector_year = Window.partitionBy("sector_key", "year")

    # ---------- 5. Window theo year (tổng toàn ngành - chỉ dòng sector-level) ----------
    w_year = Window.partitionBy("year")

    with_window = (
        resolved
        # lag theo year để lấy giá trị & năm của dòng liền trước cùng "name"
        .withColumn("_prev_year", F.lag("year", 1).over(w_name_year))
        .withColumn("_prev_value", F.lag("investment_value", 1).over(w_name_year))
        # chỉ nhận giá trị năm trước khi đúng là year - 1 (tránh trường hợp dữ liệu bị thiếu năm)
        .withColumn(
            "investment_value_pre_year",
            F.when(F.col("_prev_year") == F.col("year") - 1, F.col("_prev_value")),
        )
        # tổng theo sector (chỉ cộng các dòng có sub_sector_key)
        .withColumn(
            "total_sector_value",
            F.sum(
                F.when(F.col("sub_sector_key").isNotNull(), F.col("investment_value"))
            ).over(w_sector_year),
        )
        # tổng toàn ngành theo năm (chỉ cộng các dòng sector-level, không có sub_sector_key)
        .withColumn(
            "total_all_value",
            F.sum(
                F.when(F.col("sub_sector_key").isNull(), F.col("investment_value"))
            ).over(w_year),
        )
        .drop("_prev_year", "_prev_value")
    )

    # ---------- 6. Join dim_time + tính các cột phái sinh ----------
    result = (
        with_window.alias("wp")
        .join(
            dim_time.alias("dt"),
            (F.col("wp.year") == F.col("dt.year"))
            & (F.col("dt.quarter").isNull()),
            "left",
        )
        .select(
            F.col("dt.time_key").cast("int").alias("time_key"),

            F.col("wp.sector_key").cast("int").alias("sector_key"),

            F.col("wp.sub_sector_key").cast("int").alias("sub_sector_key"),

            F.col("wp.unit").cast("string").alias("unit"),

            F.col("wp.investment_value").cast("float").alias("investment_value"),

            F.col("wp.investment_value_pre_year")
                .cast("float")
                .alias("investment_value_pre_year"),

            (
                F.when(
                    (F.col("wp.investment_value_pre_year").isNull())
                    | (F.col("wp.investment_value_pre_year") == 0),
                    None,
                ).otherwise(
                    (
                        F.col("wp.investment_value")
                        - F.col("wp.investment_value_pre_year")
                    )
                    / F.col("wp.investment_value_pre_year")
                )
            ).cast("float").alias("yoy_growth_rate"),

            (
                F.when(
                    (F.col("wp.sub_sector_key").isNotNull())
                    & (F.col("wp.total_sector_value").isNotNull())
                    & (F.col("wp.total_sector_value") != 0),
                    F.col("wp.investment_value")
                    / F.col("wp.total_sector_value"),
                ).otherwise(None)
            ).cast("float").alias("sector_share_pct"),

            (
                F.when(
                    (F.col("wp.total_all_value").isNotNull())
                    & (F.col("wp.total_all_value") != 0),
                    F.col("wp.investment_value")
                    / F.col("wp.total_all_value"),
                ).otherwise(None)
            ).cast("float").alias("all_sector_share_pct"),
        )
    )

    result.show(100)    
    result.write\
        .format('delta')\
            .mode('overwrite')\
                .save('s3a://gold/fact_investment_by_sector')

def main_build_fact_investment_by_sector():
    spark = get_spark()
    load_investment_by_sector(spark) 