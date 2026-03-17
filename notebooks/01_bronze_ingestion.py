# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC This notebook implements the Bronze layer of the Medallion Architecture using
# MAGIC Spark Declarative Pipelines (DLT) with Auto Loader for incremental ingestion.
# MAGIC
# MAGIC **Pattern**: Raw data lands here with minimal transformation — just metadata enrichment.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "medallion_demo"
BRONZE_SCHEMA = "bronze"
RAW_DATA_PATH = "/Volumes/medallion_demo/raw/landing/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Orders (Streaming — Auto Loader)
# MAGIC
# MAGIC Incrementally ingest order events from cloud storage with schema evolution.

# COMMAND ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw order events ingested via Auto Loader with schema evolution",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "order_date"
    }
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{RAW_DATA_PATH}/orders/_schema")
        .load(f"{RAW_DATA_PATH}/orders/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_pipeline_id", lit(spark.conf.get("pipelines.id", "unknown")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Customers (Streaming — Auto Loader with CDC)
# MAGIC
# MAGIC Customer profile updates arrive as CDC events (insert/update/delete).

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer CDC events from upstream CRM system",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{RAW_DATA_PATH}/customers/_schema")
        .load(f"{RAW_DATA_PATH}/customers/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Products (Batch — Periodic Full Load)
# MAGIC
# MAGIC Product catalog loaded as periodic snapshots.

# COMMAND ----------

@dlt.table(
    name="bronze_products",
    comment="Raw product catalog snapshots",
    table_properties={"quality": "bronze"}
)
def bronze_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{RAW_DATA_PATH}/products/_schema")
        .load(f"{RAW_DATA_PATH}/products/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Clickstream (High-Volume Streaming)
# MAGIC
# MAGIC Website interaction events — highest volume source.

# COMMAND ----------

@dlt.table(
    name="bronze_clickstream",
    comment="Raw website clickstream events",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_timestamp"
    }
)
def bronze_clickstream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{RAW_DATA_PATH}/clickstream/_schema")
        .load(f"{RAW_DATA_PATH}/clickstream/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
