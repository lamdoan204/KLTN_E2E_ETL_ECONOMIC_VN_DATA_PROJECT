FROM apache/hive:3.1.3

USER root

RUN apt-get update && apt-get install -y curl wget

RUN mkdir -p /opt/hive/lib

RUN wget -P /opt/hive/lib \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN wget -P /opt/hive/lib \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml