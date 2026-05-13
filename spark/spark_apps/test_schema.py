from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
builder = SparkSession.builder \
    .appName("Delta-MinIO") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = builder.getOrCreate()



import pandas as pd

data = [
    ["Nông nghiệp", 234930.893898, "Tỷ đồng", "current", 2018, 3, "Nông, lâm nghiệp và thủy sản", "Nông nghiệp"],
    ["Lâm nghiệp", 16035.67284, "Tỷ đồng", "current", 2018, 3, "Nông, lâm nghiệp và thủy sản", "Lâm nghiệp"],
    ["Thủy sản", 70385.490989, "Tỷ đồng", "current", 2018, 3, "Nông, lâm nghiệp và thủy sản", "Thủy sản"],
    ["Công nghiệp", 647412.731544, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Công nghiệp"],
    ["Khai khoáng", 166234.681574, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Khai khoáng"],
    ["Công nghiệp chế biến, chế tạo", 367883.62793, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Công nghiệp chế biến, chế tạo"],
    ["Sản xuất và phân phối điện, khí đốt, nước", 100520.886706, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Sản xuất và phân phối điện, khí đốt, nước"],
    ["Cung cấp nước; hoạt động quản lý và xử lý rác", 12773, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Cung cấp nước, hoạt động quản lý và xử lý rác"],
    ["Xây dựng", 119386, "Tỷ đồng", "current", 2018, 3, "Công nghiệp và xây dựng", "Xây dựng"],
    ["Bán buôn và bán lẻ; sửa chữa ô tô, mô tô, xe", 240383.133484, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Bán buôn và bán lẻ, sửa chữa ô tô, mô tô, xe"],
    ["Vận tải, kho bãi", 61265.095682, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Vận tải, kho bãi"],
    ["Dịch vụ lưu trú và ăn uống", 90800.211336, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Dịch vụ lưu trú và ăn uống"],
    ["Thông tin và truyền thông", 15279.616277, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Thông tin và truyền thông"],
    ["Hoạt động tài chính, ngân hàng và bảo hiểm", 90124.99198, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động tài chính, ngân hàng và bảo hiểm"],
    ["Hoạt động kinh doanh bất động sản", 116090.10669, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động kinh doanh bất động sản"],
    ["Hoạt động chuyên môn, khoa học và công nghệ", 26078.228666, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động chuyên môn, khoa học và công nghệ"],
    ["Hoạt động hành chính và dịch vụ hỗ trợ", 9119.863286, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động hành chính và dịch vụ hỗ trợ"],
    ["Hoạt động của Đảng Cộng sản, tổ chức chính trị", 65680.333494, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động của Đảng Cộng sản, tổ chức chính trị"],
    ["Giáo dục và đào tạo", 101808.399859, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Giáo dục và đào tạo"],
    ["Y tế và hoạt động trợ giúp xã hội", 71546.098388, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Y tế và hoạt động trợ giúp xã hội"],
    ["Nghệ thuật, vui chơi và giải trí", 13973.205393, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Nghệ thuật, vui chơi và giải trí"],
    ["Hoạt động dịch vụ khác", 43252.648836, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động dịch vụ khác"],
    ["Hoạt động làm thuê các công việc trong các hộ gia đình", 3940.573128, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Hoạt động làm thuê các công việc trong các hộ gia đình"],
    ["Thuế sản phẩm trừ trợ cấp sản phẩm", 232653.752648, "Tỷ đồng", "current", 2018, 3, "Dịch vụ", "Thuế sản phẩm trừ trợ cấp sản phẩm"]
]

columns = [
    "sector_and_sub_sector",
    "current_value",
    "unit",
    "type",
    "year",
    "quarter",
    "sector",
    "sub_sector"
]

df = pd.DataFrame(data, columns=columns)

# thêm cột ingest_at
df["ingest_at"] = pd.Timestamp.now()



spark_df =spark.createDataFrame(df)
spark_df.select('sector', 'sub_sector', 'year', 'quarter', 'current_value', 'type', 'unit', 'ingest_at').show()