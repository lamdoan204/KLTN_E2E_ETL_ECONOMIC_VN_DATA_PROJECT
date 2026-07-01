# Dùng chung image với spark-master để đảm bảo Spark/Hadoop/Delta JARs
# và phiên bản Python/Spark khớp 100% với cluster -> tránh lỗi version mismatch
# khi container này submit job vào spark://spark-master:7077
FROM kltn_e2e_etl_economic_vn_data_project-spark-master

USER root

# Cài Streamlit + lib cho dashboard (pandas/pyarrow/numpy đã có sẵn từ image spark-master,
# nhưng khai báo lại trong requirements.txt để pin version rõ ràng và tránh phụ thuộc ngầm)
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# FIX QUAN TRỌNG: một trong các package ở trên (hoặc image base) có thể kéo
# theo "pyspark" mới nhất từ PyPI (ví dụ 4.x) như transitive dependency, và
# bản đó bị ưu tiên hơn pyspark built-in của $SPARK_HOME/python (3.5.8) trong
# sys.path. Khi version Python (pyspark) lệch với version JAR thật trên đĩa,
# JVM gateway (Py4J) không tìm thấy đúng class -> lỗi
# "'JavaPackage' object is not callable" khi gọi enableHiveSupport()/getOrCreate().
#
# Cách fix: gỡ sạch pyspark cài qua pip (nếu có), để Python fallback dùng đúng
# bản pyspark nằm trong $SPARK_HOME/python (luôn khớp 100% với JAR).
# ---------------------------------------------------------------------------

# Thư mục chứa code dashboard
RUN mkdir -p /opt/streamlit/app
WORKDIR /opt/streamlit/app

COPY shared/ /opt/streamlit/app/shared/

COPY app.py /opt/streamlit/app/app.py

COPY docker-entrypoint.sh /opt/streamlit/app/docker-entrypoint.sh
RUN chmod +x /opt/streamlit/app/docker-entrypoint.sh

# Spark client cần ghi log/event vào đây khi chạy local ivy resolution
RUN mkdir -p /tmp/ivy && chmod 777 /tmp/ivy

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=5 \
    CMD curl -f http://localhost:8501/_stockcheck/health || curl -f http://localhost:8501/ || exit 1

# Quay lại user spark (non-root) giống các service spark khác trong stack,
# vì image base apache/spark đã tạo user "spark" sẵn
USER spark

ENTRYPOINT ["/opt/streamlit/app/docker-entrypoint.sh"]