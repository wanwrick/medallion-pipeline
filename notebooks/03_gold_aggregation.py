# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — Business-Ready Analytics
# MAGIC
# MAGIC This notebook creates the Gold layer with:
# MAGIC - **Star Schema** design for analytical queries
# MAGIC - **Pre-aggregated metrics** for dashboard performance
# MAGIC - **Liquid Clustering** for optimized query patterns
# MAGIC - **Materialized Views** for common business questions

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, sum, count, countDistinct, avg, max, min,
    date_trunc, datediff, current_date, when, round,
    first, last, collect_set, size
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Customers (Current View)
# MAGIC
# MAGIC Flattened current-state customer dimension from SCD Type 2 history.

# COMMAND ----------

@dlt.table(
    name="dim_customers",
    comment="Current-state customer dimension for star schema joins",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def dim_customers():
    return (
        dlt.read("silver_customers")
        .filter(col("__END_AT").isNull())  # Current records only (SCD2)
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("region"),
            col("tier"),
            col("signup_date"),
            col("__START_AT").alias("effective_from"),
            datediff(current_date(), col("signup_date")).alias("customer_tenure_days"),
            when(col("tier") == "premium", True).otherwise(False).alias("is_premium")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Products

# COMMAND ----------

@dlt.table(
    name="dim_products",
    comment="Product dimension with enriched attributes",
    table_properties={"quality": "gold"}
)
def dim_products():
    return (
        dlt.read("silver_products")
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("subcategory"),
            col("unit_price"),
            when(col("unit_price") < 25, "budget")
            .when(col("unit_price") < 100, "mid-range")
            .otherwise("premium").alias("price_tier")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Date (Generated)

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension for time-series analysis",
    table_properties={"quality": "gold"}
)
def dim_date():
    return (
        spark.sql("""
            SELECT
                date AS date_key,
                year(date) AS year,
                quarter(date) AS quarter,
                month(date) AS month,
                day(date) AS day,
                dayofweek(date) AS day_of_week,
                date_format(date, 'EEEE') AS day_name,
                date_format(date, 'MMMM') AS month_name,
                weekofyear(date) AS week_of_year,
                CASE WHEN dayofweek(date) IN (1, 7) THEN true ELSE false END AS is_weekend
            FROM (
                SELECT explode(sequence(
                    to_date('2020-01-01'),
                    to_date('2026-12-31'),
                    interval 1 day
                )) AS date
            )
        """)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Orders

# COMMAND ----------

@dlt.table(
    name="fact_orders",
    comment="Order fact table joined with dimensions",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "liquid_clustering_columns": "order_date,customer_id"
    }
)
def fact_orders():
    orders = dlt.read("silver_orders")
    customers = dlt.read("dim_customers")
    products = dlt.read("dim_products")

    return (
        orders
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .select(
            orders.order_id,
            orders.order_date,
            orders.customer_id,
            orders.product_id,
            orders.quantity,
            orders.order_total,
            orders.order_status,
            customers.region,
            customers.tier.alias("customer_tier"),
            customers.is_premium,
            products.category.alias("product_category"),
            products.price_tier,
            (orders.order_total * 0.3).alias("estimated_margin"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate: Daily Revenue Metrics

# COMMAND ----------

@dlt.table(
    name="agg_daily_revenue",
    comment="Pre-aggregated daily revenue metrics for dashboards",
    table_properties={"quality": "gold"}
)
def agg_daily_revenue():
    return (
        dlt.read("fact_orders")
        .filter(col("order_status") != "cancelled")
        .groupBy(
            date_trunc("day", col("order_date")).alias("date"),
            col("region"),
            col("product_category")
        )
        .agg(
            count("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
            round(sum("order_total"), 2).alias("total_revenue"),
            round(avg("order_total"), 2).alias("avg_order_value"),
            round(sum("estimated_margin"), 2).alias("total_margin"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate: Customer Lifetime Value (LTV)

# COMMAND ----------

@dlt.table(
    name="agg_customer_ltv",
    comment="Customer lifetime value and engagement metrics",
    table_properties={"quality": "gold"}
)
def agg_customer_ltv():
    return (
        dlt.read("fact_orders")
        .filter(col("order_status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("order_total"), 2).alias("lifetime_revenue"),
            round(avg("order_total"), 2).alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            countDistinct("product_category").alias("categories_purchased"),
            datediff(max("order_date"), min("order_date")).alias("active_days"),
        )
        .withColumn(
            "customer_segment",
            when(col("lifetime_revenue") > 5000, "VIP")
            .when(col("lifetime_revenue") > 1000, "High Value")
            .when(col("lifetime_revenue") > 200, "Regular")
            .otherwise("Low Value")
        )
        .withColumn(
            "churn_risk",
            when(datediff(current_date(), col("last_order_date")) > 90, "High")
            .when(datediff(current_date(), col("last_order_date")) > 30, "Medium")
            .otherwise("Low")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate: Product Performance

# COMMAND ----------

@dlt.table(
    name="agg_product_performance",
    comment="Product-level performance metrics",
    table_properties={"quality": "gold"}
)
def agg_product_performance():
    return (
        dlt.read("fact_orders")
        .filter(col("order_status") != "cancelled")
        .groupBy("product_id", "product_category", "price_tier")
        .agg(
            count("order_id").alias("times_ordered"),
            countDistinct("customer_id").alias("unique_buyers"),
            round(sum("order_total"), 2).alias("total_revenue"),
            round(sum("estimated_margin"), 2).alias("total_margin"),
            round(avg("order_total"), 2).alias("avg_order_value"),
        )
        .withColumn(
            "margin_pct",
            round(col("total_margin") / col("total_revenue") * 100, 1)
        )
    )
