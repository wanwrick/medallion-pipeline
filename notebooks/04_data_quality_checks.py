# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Data Quality Validation
# MAGIC
# MAGIC Standalone quality checks that run as a scheduled job task.
# MAGIC Results are written to a quality metrics table for dashboard monitoring.

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, lit, current_timestamp, round
)
from datetime import datetime

# COMMAND ----------

CATALOG = "medallion_demo"
QUALITY_TABLE = f"{CATALOG}.gold.data_quality_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check Functions

# COMMAND ----------

def check_completeness(table_name, columns):
    """Check for NULL values in critical columns."""
    df = spark.table(f"{CATALOG}.{table_name}")
    total = df.count()
    results = []
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        completeness = round((1 - null_count / max(total, 1)) * 100, 2)
        results.append({
            "table": table_name,
            "check_type": "completeness",
            "column": col_name,
            "metric_value": completeness,
            "threshold": 99.0,
            "status": "PASS" if completeness >= 99.0 else "FAIL",
            "details": f"{null_count} nulls out of {total} rows"
        })
    return results

def check_uniqueness(table_name, key_column):
    """Check for duplicate keys."""
    df = spark.table(f"{CATALOG}.{table_name}")
    total = df.count()
    distinct = df.select(key_column).distinct().count()
    uniqueness = round((distinct / max(total, 1)) * 100, 2)
    return [{
        "table": table_name,
        "check_type": "uniqueness",
        "column": key_column,
        "metric_value": uniqueness,
        "threshold": 100.0,
        "status": "PASS" if uniqueness == 100.0 else "FAIL",
        "details": f"{total - distinct} duplicates found"
    }]

def check_freshness(table_name, timestamp_column, max_hours=24):
    """Check data freshness against threshold."""
    df = spark.table(f"{CATALOG}.{table_name}")
    latest = df.agg({timestamp_column: "max"}).collect()[0][0]
    if latest:
        hours_old = (datetime.now() - latest).total_seconds() / 3600
        is_fresh = hours_old <= max_hours
    else:
        hours_old = -1
        is_fresh = False
    return [{
        "table": table_name,
        "check_type": "freshness",
        "column": timestamp_column,
        "metric_value": round(hours_old, 1),
        "threshold": max_hours,
        "status": "PASS" if is_fresh else "FAIL",
        "details": f"Last update: {latest}"
    }]

def check_row_count(table_name, min_expected):
    """Check minimum row count."""
    total = spark.table(f"{CATALOG}.{table_name}").count()
    return [{
        "table": table_name,
        "check_type": "row_count",
        "column": "*",
        "metric_value": total,
        "threshold": min_expected,
        "status": "PASS" if total >= min_expected else "FAIL",
        "details": f"{total} rows (min: {min_expected})"
    }]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Quality Checks

# COMMAND ----------

all_results = []

# Silver Orders
all_results += check_completeness("silver.silver_orders", ["order_id", "customer_id", "order_total"])
all_results += check_uniqueness("silver.silver_orders", "order_id")
all_results += check_freshness("silver.silver_orders", "_validated_at", 24)
all_results += check_row_count("silver.silver_orders", 50000)

# Silver Products
all_results += check_completeness("silver.silver_products", ["product_id", "product_name"])
all_results += check_uniqueness("silver.silver_products", "product_id")

# Gold Aggregates
all_results += check_row_count("gold.agg_daily_revenue", 100)
all_results += check_row_count("gold.agg_customer_ltv", 1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results to Quality Metrics Table

# COMMAND ----------

from pyspark.sql import Row

results_df = spark.createDataFrame([
    Row(
        table_name=r["table"],
        check_type=r["check_type"],
        column_name=r["column"],
        metric_value=float(r["metric_value"]),
        threshold=float(r["threshold"]),
        status=r["status"],
        details=r["details"],
        checked_at=datetime.now()
    )
    for r in all_results
])

results_df.write.mode("append").saveAsTable(QUALITY_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

total_checks = len(all_results)
passed = sum(1 for r in all_results if r["status"] == "PASS")
failed = sum(1 for r in all_results if r["status"] == "FAIL")

print("=" * 60)
print("🛡️  DATA QUALITY REPORT")
print("=" * 60)
print(f"  Total Checks:  {total_checks}")
print(f"  ✅ Passed:     {passed}")
print(f"  ❌ Failed:     {failed}")
print(f"  Score:         {round(passed/total_checks*100, 1)}%")
print("=" * 60)

if failed > 0:
    print("\n⚠️  FAILED CHECKS:")
    for r in all_results:
        if r["status"] == "FAIL":
            print(f"  - {r['table']}.{r['column']} ({r['check_type']}): {r['details']}")
