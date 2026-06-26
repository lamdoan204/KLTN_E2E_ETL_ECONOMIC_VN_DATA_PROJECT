
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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



def create_time_key(df):

    return (
        df.withColumn(
            "time_key",
            F.concat(
                F.col("year").cast("string"),
                F.lpad(F.coalesce(F.col("quarter"), F.lit(0)).cast("string"), 2, "0"),
                F.lpad(F.coalesce(F.col("month"), F.lit(0)).cast("string"), 2, "0"),
                F.lpad(F.coalesce(F.col("day"), F.lit(0)).cast("string"), 2, "0")
            ).cast("int")
        )
    )
from functools import reduce

def build_dim_time():

    tables = []

    # GDP
    tables.append(
        spark.table("silver.gdp")
        .select(
            "year",
            "quarter",
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day")
        )
    )

    # investment
    tables.append(
        spark.table("silver.investment")
        .select(
            "year",
            "quarter",
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day")
        )
    )

    # international trade
    tables.append(
        spark.table("silver.international_ecommerce")
        .select(
            "year",
            "quarter",
            "month",
            F.lit(None).cast("int").alias("day")
        )
    )

    # industry product
    tables.append(
        spark.table("silver.industry_product")
        .select(
            "year",
            "quarter",
            "month",
            F.lit(None).cast("int").alias("day")
        )
    )

    tables.append(
        spark.table("silver.forestry")
        .select(
            "year",
            "quarter",
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day")
        )
    )
    tables.append(
        spark.table("silver.livestock")
        .select(
            "year",
            "quarter",
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day")
        )
    )
    tables.append(
        spark.table("silver.aquatic_products")
        .select(
            "year",
            "quarter",
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day")
        )
    )
    # yearly tables
    yearly_tables = [
        "annual_crops",
        "staple_crops",
        "perennial_crops",
    ]

    for tbl in yearly_tables:
        tables.append(
            spark.table(f"silver.{tbl}")
            .select(
                "year",
                F.lit(None).cast("int").alias("quarter"),
                F.lit(None).cast("int").alias("month"),
                F.lit(None).cast("int").alias("day")
            )
        )

    dim_time = reduce(lambda x, y: x.union(y), tables)

    dim_time = dim_time.distinct()

    dim_time = create_time_key(dim_time)

    dim_time = dim_time.select(
        "time_key",
        "year",
        "quarter",
        "month",
        "day"
    )

    dim_time.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://gold/dim_time")
    del dim_time

def build_dim_sector():

    sector = (
        spark.table("silver.gdp")
        .select("sector")
        .distinct()
        .withColumnRenamed("sector", "sector_name")
    )

    w = Window.orderBy("sector_name")

    sector = sector.withColumn(
        "sector_key",
        F.row_number().over(w)
    )

    sector.select(
        "sector_key",
        "sector_name"
    ).write.format("delta") \
     .mode("overwrite") \
     .save("s3a://gold/dim_sector")
    del sector
def build_dim_sub_sector():

    sector = spark.table("gold.dim_sector")

    sub = (
        spark.table("silver.gdp")
        .select("sector", "sub_sector")
        .distinct()
        .join(
            sector,
            sector.sector_name == F.col("sector")
        )
    )

    w = Window.orderBy("sub_sector")

    sub = sub.withColumn(
        "sub_sector_key",
        F.row_number().over(w)
    )

    sub.select(
        "sub_sector_key",
        F.col("sub_sector").alias("sub_sector_name"),
        "sector_key"
    ).write.format("delta") \
     .mode("overwrite") \
     .save("s3a://gold/dim_sub_sector")
    del sub 

    
def build_dim_product():

    p1 = (
        spark.table("silver.industry_product")
        .select(
            F.col("product_name"),
            F.lit("not available").alias("product_type"),
            F.lit("Industry Product").alias("product_category")
        )
    )

    p2 = (
        spark.table("silver.international_ecommerce")
        .select(
            F.col("product_name"),
            F.lit("Trade International Product").alias("product_category"),
            F.col("type").alias("product_type")
        )
    )

    p3 = (
        spark.table("silver.aquatic_products")
        .select(
            F.col("product_name"),
            F.col("aquatic_type").alias("product_type"),
            F.lit("Aquatic Product").alias("product_category")
        )
    )

    p4 = (
        spark.table("silver.forestry")
        .select(
            F.col('forestry_indicator').alias('product_name'),
            F.lit("Forestry Product").alias("product_category"),
            F.lit("not available").alias("product_type")
        )
    )
    
    p5 = (
        spark.table("silver.livestock")
        .select(
            F.col("livestock_indicator").alias("product_name"),
            F.lit("not available").alias('product_type'),
            F.lit("Livestock Product").alias('product_category')
        )
    )
    dim = p1.unionByName(p2).unionByName(p3).unionByName(p4).unionByName(p5).distinct()

    w = Window.orderBy("product_name")

    dim = dim.withColumn(
        "product_key",
        F.row_number().over(w)
    )

    dim.select(
        "product_key",
        "product_name",
        "product_type",
        "product_category"
    ).write.format("delta") \
     .mode("overwrite") \
     .save("s3a://gold/dim_product")
    del dim
def build_dim_crop():

    annual = (
        spark.table("silver.annual_crops")
        .select(
            "crop_name",
            
            F.lit("Annual").alias("crop_category")
        )
    )

    staple = (
        spark.table("silver.staple_crops")
        .select(
            "crop_name",
            F.lit("Staple").alias("crop_category")
        )
    )

    perennial = (
        spark.table("silver.perennial_crops")
        .select(
            "crop_name",
            F.lit("Perennial").alias("crop_category")
        )
    )

    dim = annual.union(staple).union(perennial).distinct()

    

    w = Window.orderBy("crop_name")

    dim = dim.withColumn(
        "crop_key",
        F.row_number().over(w)
    )

    dim.select(
        "crop_key",
        "crop_name",
        "crop_category"
    ).write.format("delta") \
     .mode("overwrite") \
     .save("s3a://gold/dim_crop")
    del dim
     

def build_dim_capital_source():

    dim = (
        spark.table("silver.investment")
        .select(
            F.col("investment_name").alias("source_name")
        )
        .distinct()
    )

    w = Window.orderBy("source_name")

    dim = dim.withColumn(
        "capital_source_key",
        F.row_number().over(w)
    )

    dim.select(
        "capital_source_key",
        "source_name"
    ).write.format("delta") \
     .mode("overwrite") \
     .save("s3a://gold/dim_capital_source")
    del dim

def calc_growth(current, previous):
    return (
        F.when(previous.isNull(), None)
         .when(previous == 0, None)
         .otherwise(((current-previous)/previous)*100)
    )

def get_time_key(df):
    return df.join(
        spark.table("gold.dim_time"),
        ["year","quarter","month","day"]
    )

def main_build_dim():
    print('LOADING DIM-TABLE')
    print('Loading dim-time')
    build_dim_time()
    print('loading dim-product')
    build_dim_product()
    print('loading dim-sector')
    build_dim_sector()
    print('loading dim-sub_sector')
    build_dim_sub_sector()
    print('loading dim-crop')
    build_dim_crop()
    print('loading dim-capital_source')
    build_dim_capital_source()
    
    spark.stop()
    
    

