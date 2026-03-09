````markdown
# Fashion Data Platform

A local lakehouse-style data platform for fashion retail analytics, built on macOS using Docker Compose, Apache Spark, Airflow, and MinIO.

This project demonstrates how a modern data platform can ingest raw data, transform it through structured layers, validate data quality, and produce business-ready analytics datasets.

---

# Project Overview

The Fashion Data Platform is a local analytics engineering project that simulates a real company data platform.

It follows a **lakehouse architecture** with layered data processing:

Raw → Bronze → Silver → Gold

Each layer progressively improves data quality and structure.

The project demonstrates practical skills in:

- Data Engineering
- Spark batch processing
- Airflow orchestration
- Lakehouse architecture
- Data quality validation
- Analytics modeling
- Automated testing
- CI with GitHub Actions

---

# Business Goal

The business goal is to transform raw online retail transaction data into trusted analytics tables that support business decisions.

Example business questions this platform answers:

- How much revenue is generated each day?
- Which products perform best?
- Which countries produce the most revenue?
- Which customers are the most valuable?

---

# Tech Stack

This project uses a modern analytics engineering stack.

| Tool | Purpose |
|-----|------|
| Docker Compose | Local platform orchestration |
| Apache Airflow | Pipeline orchestration |
| Apache Spark | Data processing |
| MinIO | S3-compatible object storage |
| PostgreSQL | Airflow metadata database |
| Great Expectations | Data quality validation |
| Pytest | Automated testing |
| GitHub Actions | Continuous Integration |

---

# Architecture Diagram

```mermaid
flowchart LR

A[Raw CSV Dataset] --> B[MinIO Raw Layer]

B --> C[Bronze Ingest Spark Job]
C --> D[MinIO Bronze Layer]

D --> E[Silver Transform Spark Job]
E --> F[MinIO Silver Layer]

F --> G[Great Expectations Validation]
G --> H[Quality Reports]

F --> I[Gold Marts Spark Job]
I --> J[MinIO Gold Layer]

K[Airflow DAG] --> C
K --> E
K --> G
K --> I

L[Spark Master] --> C
L --> E
L --> I

M[Spark Worker] --> L
N[Postgres] --> K
````

---

# Data Layers

## Raw Layer

Raw is the **source-of-truth** data layer.

Purpose:

* store original data
* allow full reprocessing later
* keep data unchanged

Example path:

```
s3a://lake/raw/online_retail/online_retail.csv
```

---

## Bronze Layer

Bronze converts raw CSV files into structured Parquet format.

Purpose:

* improve performance
* make data easier to process
* preserve original structure

Output path pattern:

```
s3a://lake/bronze/online_retail/ingest_date=<ds>/
```

---

## Silver Layer

Silver cleans and standardizes the Bronze data.

Purpose:

* remove invalid rows
* enforce basic data quality rules
* produce trusted transactional data

Current rules include:

* Quantity > 0
* UnitPrice >= 0
* Deduplication

Output path pattern:

```
s3a://lake/silver/online_retail/ingest_date=<ds>/
```

---

## Silver Validation

Silver data is validated using **Great Expectations**.

Purpose:

* enforce data quality rules
* detect data problems early
* produce validation reports

Report location:

```
s3a://lake/quality_reports/online_retail/<ds>/
```

---

## Gold Layer

Gold contains **business analytics tables** used for reporting.

Current Gold marts:

### daily_revenue

Daily revenue aggregated from transactions.

### top_products

Products ranked by revenue and sales volume.

### country_performance

Revenue aggregated by country.

### customer_rfm

Customer segmentation metrics using:

* Recency
* Frequency
* Monetary value

---

# Repository Structure

```
fashion-data-platform/

airflow/
  dags/
    silver_online_retail_dag.py

config/
  dev.yml

spark/
  jobs/
    bronze_ingest.py
    silver_transform.py
    validate_silver_ge.py
    gold_marts.py

    lib/
      silver_online_retail.py
      gold_online_retail.py

  tests/
    test_customer_rfm.py
    test_silver_online_retail.py

great_expectations/
  expectations/
    silver_online_retail.json

.github/
  workflows/
    ci.yml

docker-compose.yml
README.md
```

---

# Platform Services

After starting the platform, the following UIs are available:

| Service         | URL                                            |
| --------------- | ---------------------------------------------- |
| Airflow UI      | [http://localhost:8080](http://localhost:8080) |
| Spark Master UI | [http://localhost:8081](http://localhost:8081) |
| MinIO Console   | [http://localhost:9001](http://localhost:9001) |

These interfaces allow you to monitor the platform.

---

# Starting the Platform

Start all services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

Stop the platform:

```bash
docker compose down
```

---

# Running Spark Jobs Manually

Example: Bronze ingestion

```bash
docker compose exec spark-master spark-submit \
  /opt/spark/work-dir/spark/jobs/bronze_ingest.py \
  --config /opt/spark/work-dir/config/dev.yml \
  --date 2024-01-01
```

Example: Silver transform

```bash
docker compose exec spark-master spark-submit \
  /opt/spark/work-dir/spark/jobs/silver_transform.py \
  --config /opt/spark/work-dir/config/dev.yml \
  --date 2024-01-01
```

Example: Gold marts

```bash
docker compose exec spark-master spark-submit \
  /opt/spark/work-dir/spark/jobs/gold_marts.py \
  --config /opt/spark/work-dir/config/dev.yml \
  --date 2024-01-01
```

---

# Testing

Tests are run inside the Spark container.

Run tests:

```bash
docker compose exec \
-e PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/spark \
spark-master pytest /opt/spark/tests -q
```

Expected output:

```
4 passed
```

---

# Airflow Pipeline

The pipeline order is:

```
bronze_ingest
→ silver_transform
→ validate_silver
→ gold_marts
```

The DAG is located at:

```
airflow/dags/silver_online_retail_dag.py
```

---

# CI Pipeline

This project includes **GitHub Actions CI**.

Workflow location:

```
.github/workflows/ci.yml
```

CI runs automatically:

* on push to main
* on push to feature branches
* on pull requests

It runs **pytest automatically** to detect errors early.

---

# Debugging Tips

### Python import errors

Example:

```
ModuleNotFoundError: No module named 'spark'
```

Fix by running tests with correct Python path:

```
PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/spark
```

---

### Spark Python mismatch

Spark driver and worker must use the **same Python version**.

If versions differ, Spark jobs will fail.

---

### Wrong config path

Correct container config path:

```
/opt/fdp/config/dev.yml
```

---

# Why This Project Matters

This repository demonstrates a realistic analytics engineering platform including:

* distributed processing
* orchestration
* storage
* validation
* testing
* CI automation

It follows best practices used in modern data teams.

---

# Future Improvements

Possible next improvements:

* additional Gold layer tests
* Gold data quality validation
* dashboards or BI layer
* production deployment patterns

---

# Author

**Behzad Moloudi**

Master’s of Intractive Technologies & AI
Turku University of Applied Sciences

Focus areas demonstrated:

* Data Engineering
* Spark Processing
* Airflow Orchestration
* Lakehouse Architecture
* Analytics Engineering

````