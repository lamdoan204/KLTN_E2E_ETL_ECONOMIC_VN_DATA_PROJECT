#!/bin/bash
# docker-entrypoint.sh
# Tự động set PYTHONPATH đúng version py4j có sẵn trong $SPARK_HOME, để Python
# luôn dùng module "pyspark" đồng bộ với JAR thật trên đĩa (tránh mismatch
# gây lỗi "'JavaPackage' object is not callable").
set -e

PY4J_ZIP=$(find "${SPARK_HOME:-/opt/spark}/python/lib" -name "py4j-*.zip" | head -n 1)

if [ -z "$PY4J_ZIP" ]; then
    echo "CẢNH BÁO: Không tìm thấy py4j-*.zip trong \$SPARK_HOME/python/lib." >&2
    echo "PySpark có thể không hoạt động đúng." >&2
else
    export PYTHONPATH="${SPARK_HOME:-/opt/spark}/python:${PY4J_ZIP}:${PYTHONPATH}"
    echo "Đã set PYTHONPATH với py4j: ${PY4J_ZIP}"
fi

exec streamlit run /opt/streamlit/app/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false