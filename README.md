# 🏗️ Medallion Architecture Pipeline — Databricks Lakehouse

> A production-grade data pipeline implementing the **Bronze → Silver → Gold** medallion architecture on Databricks using Spark Declarative Pipelines (DLT), Unity Catalog governance, and multi-task job orchestration.

![Architecture](docs/architecture.png)

---

## 🎯 What This Demonstrates

- **Medallion Architecture**: Three-layer data refinement pattern (raw → validated → business-ready)
- **Spark Declarative Pipelines (DLT)**: Streaming and batch ingestion with Auto Loader
- **Change Data Capture (CDC)**: Tracking and applying incremental changes with AUTO CDC
- **Unity Catalog Governance**: Fine-grained access control, lineage tracking, and audit logging
- **Job Orchestration**: Multi-task DAG with conditional execution and event-driven triggers
- **Asset Bundles (DAB)**: Multi-environment deployment (dev → staging → prod)
- **Data Quality Checks**: Expectation-based quality enforcement at every layer

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Databricks Workspace                         │
│                                                                     │
│  ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐  │
│  │  Sources   │───▶│  BRONZE   │───▶│  SILVER   │───▶│   GOLD    │  │
│  │           │    │  (Raw)    │    │(Validated)│    │(Business) │  │
│  │ • CSV     │    │           │    │           │    │           │  │
│  │ • JSON    │    │ Auto      │    │ CDC +     │    │ Star      │  │
│  │ • Kafka   │    │ Loader    │    │ SCD Type2 │    │ Schema    │  │
│  │ • API     │    │ Streaming │    │ Quality   │    │ Aggregates│  │
│  └───────────┘    └───────────┘    └───────────┘    └───────────┘  │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Unity Catalog                             │    │
│  │  Catalog: medallion_demo                                    │    │
│  │  Schemas: bronze | silver | gold                            │    │
│  │  Access: RBAC + Column Masking + Row Filters               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Job Orchestration                         │    │
│  │  Task 1: Ingest → Task 2: Transform → Task 3: Quality      │    │
│  │  Trigger: Scheduled (cron) + File Arrival                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
medallion-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py       # Auto Loader + streaming tables
│   ├── 02_silver_transformation.py  # CDC, SCD Type 2, quality checks
│   ├── 03_gold_aggregation.py       # Star schema + business metrics
│   ├── 04_data_quality_checks.py    # Expectation-based validation
│   └── 05_setup_sample_data.py      # Generate sample ecommerce data
├── config/
│   ├── pipeline_config.yaml         # DLT pipeline configuration
│   ├── job_config.yaml              # Multi-task job DAG
│   └── databricks.yml               # Asset Bundle (DAB) config
├── tests/
│   ├── test_bronze.py               # Bronze layer unit tests
│   ├── test_silver.py               # Silver layer unit tests
│   └── test_gold.py                 # Gold layer unit tests
├── docs/
│   └── architecture.png
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Databricks workspace (Community Edition works)
- Databricks CLI configured (`databricks auth login`)
- Python 3.11+

### Setup

```bash
# 1. Clone this repo
git clone https://github.com/parozmehta/medallion-pipeline.git
cd medallion-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure your workspace
cp config/databricks.yml.example config/databricks.yml
# Edit with your workspace URL and catalog name

# 4. Generate sample data
databricks workspace import notebooks/05_setup_sample_data.py /Users/you/medallion-pipeline/

# 5. Create and run the DLT pipeline
databricks pipelines create --json config/pipeline_config.yaml
databricks pipelines start-update --pipeline-id <your-pipeline-id>
```

---

## 📊 Sample Data: E-Commerce Domain

The pipeline processes a simulated e-commerce dataset:

| Table | Description | Volume |
|-------|-------------|--------|
| `raw_orders` | Customer order events | ~100K rows/day |
| `raw_customers` | Customer profile updates (CDC) | ~10K rows/day |
| `raw_products` | Product catalog changes | ~1K rows/day |
| `raw_clickstream` | Website interaction events | ~500K rows/day |

---

## 🔧 Key Implementation Details

### Bronze Layer — Raw Ingestion
- **Auto Loader** for incremental file ingestion with schema evolution
- **Streaming Tables** for real-time event processing
- Metadata enrichment: `_ingestion_timestamp`, `_source_file`, `_batch_id`

### Silver Layer — Validated & Enriched
- **AUTO CDC** for tracking changes in customer and product data
- **SCD Type 2** for maintaining historical customer dimension
- **Data Quality Expectations**: NOT NULL, valid ranges, referential integrity
- **Deduplication** using watermarks and windowing

### Gold Layer — Business-Ready
- **Star Schema**: Fact tables (orders, clickstream) + Dimension tables (customers, products, dates)
- **Pre-aggregated metrics**: Daily revenue, customer LTV, product performance
- **Liquid Clustering** for optimized query performance

---

## 🛡️ Governance (Unity Catalog)

```sql
-- Catalog & schema setup
CREATE CATALOG IF NOT EXISTS medallion_demo;
CREATE SCHEMA IF NOT EXISTS medallion_demo.bronze;
CREATE SCHEMA IF NOT EXISTS medallion_demo.silver;
CREATE SCHEMA IF NOT EXISTS medallion_demo.gold;

-- Row-level security on gold tables
CREATE FUNCTION gold_region_filter(region STRING)
  RETURN IF(IS_ACCOUNT_GROUP_MEMBER('north_america_team'), region = 'NA', TRUE);

-- Column masking for PII
CREATE FUNCTION mask_email(email STRING)
  RETURN CONCAT(LEFT(email, 2), '***@***', RIGHT(email, 4));
```

---

## 📈 Metrics & Monitoring

| Metric | Target | Measurement |
|--------|--------|-------------|
| Pipeline freshness | < 15 min | Time since last Gold update |
| Data quality score | > 99.5% | Expectations pass rate |
| Processing latency | < 5 min | Bronze → Gold end-to-end |
| Row count accuracy | ±0.1% | Source vs. Gold reconciliation |

---

## 🏷️ Technologies

`Databricks` `Delta Lake` `Spark Declarative Pipelines (DLT)` `Unity Catalog` `Auto Loader` `Python` `SQL` `Asset Bundles` `CDC` `SCD Type 2`

---

## 👤 Author

**Paroz Mehta** — GM, Data Warehousing & Engineering | Cornell/Smith EMBA '26

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/parozmehta)

Built with [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit)
