# Real-Estate Project — Vietnam Property Data Pipeline

A comprehensive data engineering pipeline for scraping, processing, storing, and analyzing Vietnamese real estate data from [nhadat247.com.vn](https://nhadat247.com.vn). Built with a modern data lakehouse architecture using Dagster for orchestration, Delta Lake on MinIO for storage, and PostgreSQL for analytics.

## Overview

The Vietnamese real estate market generates massive amounts of data from online sources. This project builds an end-to-end data pipeline to collect, process, and analyze that data efficiently — enabling data-driven decisions for investors, developers, and analysts.

### Goals

- Automated web scraping of real estate listings at scale
- Data lakehouse architecture with Delta Lake for ACID transactions and schema evolution
- PostgreSQL integration for relational analytics and reporting
- Analytics and visualization tooling for data exploration
- Reliable, maintainable, and scalable pipeline design

## Features

- **Web Scraping**: Multi-threaded scraping using `requests` + `BeautifulSoup` (no Selenium — avoids DNS issues on Windows)
- **Data Processing**: Cleaning, normalization, geocoding, and deduplication with Pandas
- **Data Lakehouse**: Delta Lake on MinIO (S3-compatible) with ACID transactions and automatic schema evolution
- **PostgreSQL Export**: Automated UPSERT from Delta Lake to PostgreSQL for relational analytics
- **Orchestration**: Dagster-managed pipeline with monitoring, retries, and scheduling
- **Analytics**: DuckDB-powered Jupyter notebooks and SQL scripts for market analysis

## Architecture

```
+-------------------+    +-------------------+    +-------------------+    +-------------------+    +-------------------+
|   Web Scraping    | -> |   Data Process    | -> |   Delta Lake      | -> |   PostgreSQL      | -> |   Data Explore    |
|  (Requests + BS)  |    |   (Pandas)        |    |   (MinIO S3)      |    |   (Export)        |    |   (Jupyter)       |
+-------------------+    +-------------------+    +-------------------+    +-------------------+    +-------------------+
         |                        |                        |                        |                        |
         +------------------------+------------------------+------------------------+------------------------+
                                  |                        |                        |
                     +-------------------+      +-------------------+    +-------------------+
                     |    Dagster       |      |    Analytics      |    |   SQL Queries     |
                     |  Orchestration   |      |   (DuckDB)        |    |  (PostgreSQL)     |
                     +-------------------+      +-------------------+    +-------------------+
```

### Pipeline Stages

1. **Data Ingestion** — Read search criteria from config, generate thousands of search URLs, scrape listings in parallel
2. **Data Processing** — Clean and normalize raw data (price, area, coordinates), geocode addresses, assign unique property IDs
3. **Storage** — Write processed data to Delta Lake on MinIO as Parquet with ACID transactions (new version per run)
4. **Export** — Sync Delta Lake to PostgreSQL via UPSERT logic, create indexes, verify integrity
5. **Analytics** — Run SQL queries, explore via Jupyter notebooks with DuckDB/PostgreSQL

## Tech Stack

### Core Dependencies
| Library | Purpose |
|---------|---------|
| Dagster 1.6.8 | Workflow orchestration and pipeline management |
| Dagster-DeltaLake-Pandas | Delta Lake integration with Pandas |
| Dagster-Postgres | PostgreSQL integration |
| Delta Lake | ACID transactions and time travel for data lake |
| PostgreSQL | Relational database for analytics and reporting |
| MinIO | S3-compatible object storage |
| PyArrow | Apache Arrow for data processing |
| Pandas | Data manipulation and analysis |
| DuckDB | In-process analytical database (used in notebooks) |
| SQLAlchemy | ORM for database operations |
| Requests + BeautifulSoup4 | HTTP client and HTML parsing |
| Boto3 | AWS S3 API client (MinIO compatible) |

### Development & Deployment
- **Dagstermill** — Jupyter notebook integration with Dagster
- **PostgreSQL Export Op** — Built-in Dagster operation for export

## Getting Started

### Prerequisites

- Python 3.10+
- MinIO (or any S3-compatible storage)
- PostgreSQL 14+
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/BinhTHB/Real-Estate_Project.git
cd Real-Estate_Project

# Set up virtual environment
cd src/pipelines/real-estate
python -m venv venv

# Activate (Windows)
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Start MinIO

```bash
minio server /tmp/minio/
```

MinIO will be available at:
- **API Endpoint**: http://127.0.0.1:9000
- **Username**: minioadmin
- **Password**: minioadmin

### Run the Pipeline

```bash
# Start Dagster UI
dagster dev

# Open http://127.0.0.1:3000 and run the scrape_realestate job
# Pipeline flow: Scrape -> Delta Lake -> PostgreSQL Export -> Analytics
```

## Data Schema

Each scraped listing contains the following fields:

| Field | Description |
|-------|-------------|
| url | Listing URL |
| tieu_de | Property title |
| muc_gia | Price (billions/millions VND) |
| dien_tich | Area (m2) |
| ia_chi | Full address |
| latitude / longitude | GPS coordinates |
| propertydetails_propertyid | Unique ID (hash from URL) |
| ngay_ang | Collection date |

## Operations & Deployment

### Pipeline Workflow

1. **Data Ingestion** — Reads search criteria from config, generates URLs across region/property-type/price ranges, scrapes in parallel
2. **Data Processing** — Cleans and normalizes raw data, converts price/area to numeric, geocodes addresses, assigns unique IDs
3. **Data Storage** — Writes to Delta Lake on MinIO as Parquet with ACID transactions (new version per run)
4. **Data Export** — Syncs Delta Lake to PostgreSQL via UPSERT, creates indexes, verifies integrity

### Production Deployment

**Infrastructure:**
- MinIO on dedicated server or cloud storage (AWS S3, GCS)
- PostgreSQL on managed service (AWS RDS, Google Cloud SQL)
- Dagster daemon on container orchestration platform

**Configuration:**
- Environment variables for sensitive credentials
- YAML files for pipeline configuration
- All configs under version control

**Monitoring & Alerting:**
- Real-time monitoring via Dagster UI
- Centralized logging for troubleshooting
- Alerts on pipeline failures or performance degradation

**Scheduling:**
- Scheduled daily/weekly runs
- Automated retries for failed operations
- Backup strategies for data and metadata

### Maintenance

- **Monitoring**: Track runtime, throughput, error rates, data quality, storage usage
- **Updates**: Test on staging, gradual rollout, rollback plans
- **Scalability**: Monitor resource usage, plan capacity upgrades, optimize queries

### Disaster Recovery

- **Backup**: Regular Delta Lake snapshots, PostgreSQL point-in-time recovery, cross-region replication
- **Failover**: Automatic failover for database/storage, manual procedures for complex failures
- **Business Continuity**: Alternative data sources, cached serving when pipeline is down, SLAs

### Performance Optimization

- **Processing**: Parallel processing, memory-efficient algorithms, incremental updates
- **Storage**: Partitioning by date/location, compression, query optimization with indexes
- **Network**: Connection pooling, batch operations

## Limitations

- Relies on the target website's HTML structure (may break on redesign)
- Rate limiting and anti-bot measures can affect scraping throughput
- Running MinIO locally requires sufficient system resources

## Roadmap

- Expand data sources to multiple Vietnamese real estate websites
- Deploy on cloud (AWS/GCP/Azure) with managed services
- Build ML models for property price prediction
- Create a web dashboard for real-time analytics
- Implement data quality monitoring and alerting

## Acknowledgments

- [Dagster](https://dagster.io/) — Workflow orchestration
- [Delta Lake](https://delta.io/) — Data lakehouse storage
- [PostgreSQL](https://postgresql.org/) — Relational database
- [SQLAlchemy](https://sqlalchemy.org/) — Python SQL toolkit
- [MinIO](https://min.io/) — Object storage
- [nhadat247.com.vn](https://nhadat247.com.vn) — Data source
