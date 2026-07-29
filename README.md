# END-TO-END Data Pipeline: Extract - Transform - Load Vietnam Economic Data

## Overview

This project is an end-to-end data engineering pipeline designed to collect, process, store, and serve Vietnam's macroeconomic data. The pipeline automates the extraction of official economic statistics from the General Statistics Office (GSO) of Vietnam, transforms heterogeneous datasets into standardized analytical formats, and loads them into a modern data lake architecture.

Beyond traditional data engineering, the project also prepares unstructured documents for Retrieval-Augmented Generation (RAG), enabling AI-powered chatbots to retrieve accurate economic information with improved response quality.

The primary objective is to provide investors, analysts, researchers, and other stakeholders with a centralized, reliable, and searchable source of Vietnam's economic indicators over time.

---

## Key Features

* Automated extraction of Vietnam economic data from official sources
* Scheduled workflow orchestration using Apache Airflow
* Data cleaning, transformation, and standardization
* Data lake architecture powered by Delta Lake
* Metadata management with Apache Hive
* Object storage using MinIO
* Large-scale data processing with Apache Spark
* Flexible data manipulation using Pandas
* Interactive dashboard built with Streamlit
* Processing unstructured documents for Retrieval-Augmented Generation (RAG)
* Vector embedding storage using Qdrant
* Fully containerized deployment with Docker

---

## Project Goals

The project aims to:

* Build a fully automated ETL pipeline for Vietnam economic statistics.
* Standardize heterogeneous datasets collected from official government sources.
* Provide historical economic indicators for analysis and visualization.
* Support investment research and macroeconomic analysis.
* Prepare structured and unstructured data for AI applications.
* Improve chatbot performance using Retrieval-Augmented Generation (RAG).

---

## Technology Stack

| Category                     | Technologies         |
| ---------------------------- | -------------------- |
| Workflow Orchestration       | Apache Airflow       |
| Data Collection              | BeautifulSoup        |
| Data Processing              | Apache Spark, Pandas |
| Object Storage               | MinIO                |
| Metadata Management          | Apache Hive          |
| Unstructured Data Processing | Unstructured         |
| Vector Database              | Qdrant               |
| Visualization                | Streamlit            |
| Containerization             | Docker               |
---

## Data Pipeline

The pipeline follows a fully automated **Extract – Transform – Load (ETL)** workflow orchestrated by Apache Airflow. It is designed to collect, process, and organize Vietnam's macroeconomic data into a scalable data lake while simultaneously preparing unstructured documents for Retrieval-Augmented Generation (RAG).

### 1. Extract

The extraction layer automatically collects economic reports and statistical datasets published by the **General Statistics Office (GSO) of Vietnam**.

During this stage, the pipeline:

* Crawls newly published reports and statistical tables.
* Downloads Excel files and Gets Context of reports.
* Preserves raw files without modification for auditing and reproducibility.
* Stores raw datasets in the Bronze layer of the data lake.

---

### 2. Transform

The transformation layer converts heterogeneous raw data into standardized analytical datasets.

For **structured data**, the pipeline:

* Cleans and validates extracted datasets.
* Standardizes schemas, indicator names, and product names.
* Resolves inconsistencies across different reporting periods.
* Builds normalized datasets suitable for analytical processing.

For **unstructured data**, the pipeline:

* Parses reports context using Unstructured.
* Extracts textual content, tables, and figures.
* Chunks documents into semantic sections.
* Generates vector embeddings for downstream AI applications.

---

### 3. Load

The processed data is stored according to the Medallion Architecture.

* **Bronze Layer** stores raw source files.
* **Silver Layer** contains cleaned and standardized datasets.
* **Gold Layer** provides business-ready analytical tables designed using a Snowflake Schema.

Supporting services include:

* **MinIO** for object storage.
* **Apache Hive** for metadata management.
* **Qdrant** for vector storage used by RAG applications.
---

### 4. Analytics & Visualization

Business-ready datasets from the Gold layer are exposed through an interactive Streamlit dashboard, enabling users to:

* Explore historical economic indicators.
* Analyze macroeconomic trends.
* Monitor investment, production, trade, and agricultural statistics.
---
### 5. AI Knowledge Base

In parallel with the analytical pipeline, the project builds an AI-ready knowledge base from economic reports.

The pipeline:

* Processes unstructured data.
* Creates semantic embeddings.
* Stores vectors in Qdrant.
* Enables Retrieval-Augmented Generation (RAG) for more accurate and context-aware chatbot responses.

---

## Architecture

> Architecture diagram will be added here.

---
## Snowflake Schema in Gold Layer serve for analysis

> Schema will be added here.

---

## Project Structure

```text
.
├── build.sh                        # Build and deployment script
├── docker-compose.yaml             # Docker Compose configuration
├── config/
│   └── airflow.cfg                 # Apache Airflow configuration
│
├── dags/
│   └── init_report_data_dag.py     # Main ETL workflow
│
├── code_scripts/
│   ├── bronze/                     # Data ingestion layer
│   │   ├── crawl_and_load_newest_report.py
│   │   ├── crawl_and_load_report_excel_files_to_bronze.py
│   │   └── reuse_function.py
│   │
│   ├── silver/
│   │   ├── numeric_data/           # Structured data processing
│   │   └── context_data/           # Unstructured document processing for RAG
│   │
│   └── gold/                       # Data warehouse layer
│       ├── build_dim.py
│       ├── build_fact_gdp.py
│       ├── build_fact_crop.py
│       ├── build_fact_production.py
│       ├── build_fact_total_investment.py
│       ├── build_fact_investment_by_sector.py
│       ├── build_fact_trade_international_fact.py
│       └── load_data_to_gold_layer.py
│
├── streamlit/                      # Dashboard application
│   ├── app.py
│   └── shared/
│
├── hive/                           # Apache Hive container
├── spark/                          # Apache Spark container
├── python/                         # Python runtime container
├── parser/                         # Document parser container
└── plugins/                        # Airflow plugins
README.md
```

---

## Data Flow

```text
General Statistics Office (GSO)
            │
            ▼
     BeautifulSoup Crawler
            │
            ▼
   Spark + Pandas Processing
            │
            ▼
          MinIO
            │
     ┌──────┴──────┐
     ▼             ▼
 Apache Hive   Delta Lake
     │
     ▼
 Streamlit Dashboard

Unstructured Documents
            │
            ▼
      Unstructured
            │
            ▼
        Embeddings
            │
            ▼
         Qdrant
            │
            ▼
      RAG Chatbot
```

---

## Installation

> Installation guide will be added later.

---

## Configuration

> Environment variables and configuration instructions will be added later.

---

## Dashboard

The Streamlit dashboard provides an interactive interface for exploring Vietnam's macroeconomic indicators, enabling users to:

* Monitor historical economic trends
* Analyze key macroeconomic metrics
---

## Future Improvements

* Incremental data ingestion
* Real-time data synchronization
* Automated data quality validation
* Data lineage tracking
* CI/CD pipeline
* Monitoring and alerting
* LLM-powered economic insights
* Multi-source data integration
---


