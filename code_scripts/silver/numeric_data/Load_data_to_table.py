from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, lower

builder = SparkSession.builder \
    .appName("Delta-MinIO") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ).config(
        "spark.sql.catalogImplementation",
        "hive"
    ).config(
        "hive.metastore.uris",
        "thrift://hive:9083"
    ).config(
        "spark.sql.warehouse.dir",
        "s3a://warehouse/"
    ).config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    ).config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    ).config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin"
    ).config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    ).config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    ).config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ).config(
        "spark.driver.memory",
        "2g"
    ).config(
        "spark.driver.maxResultSize",
        "1g"
    ).config(
        "spark.sql.shuffle.partitions",
        "4"
    ).config(
        "spark.default.parallelism",
        "4"
    ).config(
        "spark.sql.adaptive.enabled",
        "true"
    ).config(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true"
    ).config(
        "spark.ui.showConsoleProgress",
        "false"
    ).enableHiveSupport()


spark = builder.getOrCreate()


# Map: với mỗi table, các cột "khóa định danh" dạng string không được phép
# null / rỗng / "nan" literal. Dùng để dropna ở tầng Spark sau khi tạo spark_df,
# như một lớp phòng thủ thứ hai (độc lập với việc dropna ở tầng pandas trước đó).
TABLE_KEY_STRING_COLS = {
    'gdp': ['sector', 'sub_sector'],
    'investment': ['investment_name'],
    'international_ecommerce': ['type', 'product_name'],
    'forestry': ['forestry_indicator'],
    'livestock': ['livestock_indicator'],
    'aquatic_products': ['aquatic_type', 'product_name'],
    'industry_product': ['product_name'],
    'annual_crops': ['crop_name'],
    'staple_crops': ['crop_name'],
    'perennial_crops': ['crop_name'],
}


def dropna_spark_df(spark_df, key_cols):
    """Loại bỏ các dòng mà bất kỳ cột trong key_cols bị null, hoặc là chuỗi
    rỗng / chỉ chứa khoảng trắng, hoặc là literal 'nan'/'none'/'null'/'nat'
    (thường sinh ra khi pandas ép kiểu str() trên NaN trước khi convert
    sang Spark DataFrame).

    key_cols: danh sách tên cột string cần đảm bảo có giá trị hợp lệ.
    Chỉ áp dụng cho các cột thực sự tồn tại trong spark_df.
    """
    empty_like_literals = ['nan', 'none', 'null', 'nat', '']

    existing_key_cols = [c for c in key_cols if c in spark_df.columns]
    if not existing_key_cols:
        return spark_df

    # Bước 1: dropna chuẩn (loại các giá trị null thật sự ở các cột khóa).
    spark_df = spark_df.dropna(subset=existing_key_cols)

    # Bước 2: loại tiếp các giá trị "rỗng giả" dạng string (sau khi trim +
    # lowercase) mà dropna() thường không bắt được, vì chúng không phải null
    # thực sự mà là literal string như 'nan', 'NaN', '', '   '.
    for c in existing_key_cols:
        spark_df = spark_df.filter(
            ~lower(trim(col(c))).isin(empty_like_literals)
        )

    return spark_df


def insert_df_to_table_silver_layer(df: pd.DataFrame, table_name, year=None, quarter=None):

    print('Bắt đầu insert dữ liệu vào SILVER layer')
    if df is None or df.empty:
        print(
            f'BỎ QUA insert vào {table_name} - {year} {quarter}: '
            f'DataFrame đầu vào rỗng hoặc None'
        )
        return
    try:
        for c in df.columns:

            if c in [
                'value',
                'production',
                'area',
                'yield',
                'quantity'
            ]:

                df[c] = pd.to_numeric(
                    df[c],
                    errors='coerce'
                ).astype(float)

            elif c in [
                'year',
                'quarter',
                'month'
            ]:

                df[c] = pd.to_numeric(
                    df[c],
                    errors='coerce'
                ).astype('int32')

            elif c == 'ingest_at':

                df[c] = pd.to_datetime(
                    df[c],
                    errors='coerce'
                )


        # ===== GDP xử lý riêng =====
        if table_name == 'gdp':

            

            spark_df = spark.createDataFrame(df)

            # Lớp dropna thứ hai ở tầng Spark, trước khi select/cast.
            spark_df = dropna_spark_df(spark_df, TABLE_KEY_STRING_COLS['gdp'])

            spark_df = (
                        spark_df
                        .select(
                            'sector',
                            'sub_sector',
                            'year',
                            'quarter',
                            'value',
                            'type',
                            'unit',
                            'ingest_at'
                        )
                        .withColumn(
                            "sector",
                            col("sector").cast("string")
                        )
                        .withColumn(
                            "sub_sector",
                            col("sub_sector").cast("string")
                        )
                        .withColumn(
                            "year",
                            col("year").cast("int")
                        )
                        .withColumn(
                            "quarter",
                            col("quarter").cast("int")
                        )
                        .withColumn(
                            "value",
                            col("value").cast("double")
                        )
                        .withColumn(
                            "type",
                            col("type").cast("string")
                        )
                        .withColumn(
                            "unit",
                            col("unit").cast("string")
                        )
                        .withColumn(
                            "ingest_at",
                            col("ingest_at").cast("timestamp")
                        )
                    )

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

            # Lớp dropna thứ hai ở tầng Spark, trước khi select/cast theo table.
            key_cols = TABLE_KEY_STRING_COLS.get(table_name, [])
            spark_df = dropna_spark_df(spark_df, key_cols)

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
                    'unit','quantity', 'quantity_unit','month',
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
        elif(table_name == 'investment_by_sector'):
            spark_df = spark_df.select(
                'name',
                'value','unit','year','ingest_at'
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