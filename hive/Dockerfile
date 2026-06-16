FROM apache/hive:3.1.3
USER root

RUN apt-get update && apt-get install -y curl wget postgresql-client

RUN mkdir -p /opt/hive/lib

# hadoop-aws phải cùng version với Hadoop trong image (3.1.3)
RUN wget -P /opt/hive/lib \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar

RUN wget -P /opt/hive/lib \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

# PostgreSQL JDBC driver
RUN wget -q -P /opt/hive/lib \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]