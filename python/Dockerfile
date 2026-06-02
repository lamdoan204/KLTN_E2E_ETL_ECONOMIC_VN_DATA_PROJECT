FROM python:3.11

RUN apt-get update && \
    apt-get install -y libreoffice && \
    apt-get clean

RUN pip3 install pandas pyarrow numpy==1.26.4 minio requests beautifulsoup4

WORKDIR /app