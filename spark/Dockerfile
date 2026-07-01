FROM apache/spark:3.5.8

USER root

# Cài đặt curl và wget
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*
# Cài pandas
RUN pip3 install pandas pyarrow numpy==1.26.4 openpyxl minio boto3



# Tạo thư mục Ivy cache
RUN mkdir -p /tmp/ivy && chmod 777 /tmp/ivy
RUN mkdir -p /opt/spark/jars-extra

WORKDIR /opt/spark/jars-extra
# Download Delta Lake packages
# Delta Lake
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

# Delta storage
RUN wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Hadoop AWS (S3)
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK (bắt buộc cho S3)
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Copy vào classpath của Spark
RUN cp /opt/spark/jars-extra/*.jar /opt//spark/jars
# Set working directory
WORKDIR /opt/spark/apps

USER spark