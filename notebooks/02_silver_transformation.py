# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Validated & Enriched Data
# MAGIC
# MAGIC This notebook implements the Silver layer with:
# MAGIC - **Data Quality Expectations** (fail, drop, or quarantine bad records)
# MAGIC - **CDC Processing** with AUTO CDC for customer dimension
# MAGIC - **SCD Type 2** for historical tracking
# MAGIC - **Deduplication** and cleansing

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, trim, lower, regexp_replace,
    to_timestamp, current_timestamp, sha2, concat_ws
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Orders (Validated + Enriched)
# MAGIC
# MAGIC Apply quality expectations and business validations.

# COMMAND ----------

@dlt.table(
    name="silver_orders",
    comment="Validated and enriched order events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "order_date,customer_id"
    }
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "order_total > 0")
@dlt.expect("valid_status", "order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')")
@dlt.expect("recent_order", "order_date >= '2020-01-01'")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("order_date", to_timestamp(col("order_date")))
        .withColumn("order_total", col("order_total").cast("decimal(10,2)"))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .withColumn(
            "order_hash",
            sha2(concat_ws("||", col("order_id"), col("customer_id"), col("order_total")), 256)
        )
        .withColumn("_validated_at", current_timestamp())
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Customers (SCD Type 2 via AUTO CDC)
# MAGIC
# MAGIC Track historical changes to customer profiles using SCD Type 2.
# MAGIC AUTO CDC automatically handles insert/update/delete operations.

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_customers",
    comment="Customer dimension with SCD Type 2 history tracking",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=col("_operation") == "DELETE",
    apply_as_truncates=None,
    stored_as_scd_type=2,
    except_column_list=["_ingestion_timestamp", "_source_file", "_operation"],
    track_history_column_list=["email", "address", "phone", "tier", "region"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Products (Validated Catalog)
# MAGIC
# MAGIC Clean and validate product catalog with business rules.

# COMMAND ----------

@dlt.table(
    name="silver_products",
    comment="Validated product catalog",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "product_name IS NOT NULL AND LENGTH(product_name) > 0")
@dlt.expect("valid_price", "unit_price > 0")
@dlt.expect("valid_category", "category IS NOT NULL")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", lower(trim(col("category"))))
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("_validated_at", current_timestamp())
        .dropDuplicates(["product_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Clickstream (Sessionized + Enriched)
# MAGIC
# MAGIC Deduplicate, validate, and enrich clickstream events.

# COMMAND ----------

@dlt.table(
    name="silver_clickstream",
    comment="Validated and enriched clickstream events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "event_timestamp,session_id"
    }
)
@dlt.expect_or_drop("valid_session", "session_id IS NOT NULL")
@dlt.expect_or_drop("valid_event", "event_type IS NOT NULL")
@dlt.expect("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_clickstream():
    return (
        dlt.read_stream("bronze_clickstream")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("page_url", lower(trim(col("page_url"))))
        .withColumn(
            "device_category",
            when(col("user_agent").contains("Mobile"), "mobile")
            .when(col("user_agent").contains("Tablet"), "tablet")
            .otherwise("desktop")
        )
        .withColumn("_validated_at", current_timestamp())
        .withWatermark("event_timestamp", "1 hour")
        .dropDuplicates(["session_id", "event_type", "event_timestamp"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine Table — Failed Quality Records
# MAGIC
# MAGIC Records that fail critical expectations are routed here for investigation.

# COMMAND ----------

@dlt.table(
    name="silver_quarantine",
    comment="Records that failed quality expectations — requires manual review",
    table_properties={"quality": "quarantine"}
)
def silver_quarantine():
    return (
        dlt.read_stream("bronze_orders")
        .filter(
            (col("order_id").isNull()) |
            (col("customer_id").isNull()) |
            (col("order_total") <= 0)
        )
        .withColumn("_quarantine_reason",
            when(col("order_id").isNull(), "missing_order_id")
            .when(col("customer_id").isNull(), "missing_customer_id")
            .when(col("order_total") <= 0, "invalid_amount")
            .otherwise("unknown")
        )
        .withColumn("_quarantined_at", current_timestamp())
    )
