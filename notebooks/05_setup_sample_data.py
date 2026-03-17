# Databricks notebook source
# MAGIC %md
# MAGIC # 📦 Setup: Generate Sample E-Commerce Data
# MAGIC
# MAGIC Run this notebook once to generate sample data for the Medallion Pipeline demo.
# MAGIC Creates realistic e-commerce data in the landing zone volumes.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "medallion_demo"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/landing"

# Create catalog and schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")

# Create volume for raw data landing
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Helpers

# COMMAND ----------

REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
TIERS = ["basic", "standard", "premium"]
CATEGORIES = ["electronics", "clothing", "home", "books", "sports", "food"]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]
DEVICES = ["Mozilla/5.0 Mobile", "Mozilla/5.0 Tablet", "Mozilla/5.0 Desktop"]

FIRST_NAMES = ["Alice", "Bob", "Carlos", "Diana", "Erik", "Fatima", "George", "Hannah",
               "Ivan", "Julia", "Kenji", "Luna", "Marco", "Nadia", "Oscar", "Priya"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Lee"]

def random_date(start_days_ago=365, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers (~5,000 records)

# COMMAND ----------

customers = []
for i in range(5000):
    cust_id = f"CUST-{str(i+1).zfill(6)}"
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    signup = random_date(730, 30)
    customers.append({
        "customer_id": cust_id,
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "region": random.choice(REGIONS),
        "tier": random.choice(TIERS),
        "address": f"{random.randint(1,9999)} {random.choice(['Main','Oak','Elm','Park'])} St",
        "signup_date": signup.strftime("%Y-%m-%d"),
        "updated_at": signup.strftime("%Y-%m-%dT%H:%M:%S"),
        "_operation": "INSERT"
    })

# Write customers as JSON files (batches of 500)
for batch_idx in range(0, len(customers), 500):
    batch = customers[batch_idx:batch_idx+500]
    batch_num = batch_idx // 500
    path = f"{VOLUME_PATH}/customers/batch_{batch_num}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(c) for c in batch), overwrite=True)

print(f"✅ Generated {len(customers)} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products (~500 records)

# COMMAND ----------

import csv
import io

products = []
for i in range(500):
    cat = random.choice(CATEGORIES)
    products.append({
        "product_id": f"PROD-{str(i+1).zfill(5)}",
        "product_name": f"{cat.title()} Item {i+1}",
        "category": cat,
        "subcategory": f"{cat}_sub_{random.randint(1,5)}",
        "unit_price": round(random.uniform(5, 500), 2),
        "supplier": f"Supplier-{random.randint(1,20)}",
    })

# Write as CSV
output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=products[0].keys())
writer.writeheader()
writer.writerows(products)
dbutils.fs.put(f"{VOLUME_PATH}/products/catalog_v1.csv", output.getvalue(), overwrite=True)

print(f"✅ Generated {len(products)} product records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Orders (~100,000 records)

# COMMAND ----------

orders = []
for i in range(100000):
    order_date = random_date(365, 0)
    cust = random.choice(customers)
    prod = random.choice(products)
    qty = random.randint(1, 10)
    orders.append({
        "order_id": f"ORD-{str(i+1).zfill(8)}",
        "customer_id": cust["customer_id"],
        "product_id": prod["product_id"],
        "quantity": qty,
        "order_total": round(prod["unit_price"] * qty, 2),
        "order_status": random.choice(STATUSES),
        "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "shipping_method": random.choice(["standard", "express", "overnight"]),
    })

# Write orders as JSON files (batches of 10,000)
for batch_idx in range(0, len(orders), 10000):
    batch = orders[batch_idx:batch_idx+10000]
    batch_num = batch_idx // 10000
    path = f"{VOLUME_PATH}/orders/batch_{batch_num}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(o) for o in batch), overwrite=True)

print(f"✅ Generated {len(orders)} order records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Clickstream (~200,000 events)

# COMMAND ----------

clickstream = []
for i in range(200000):
    ts = random_date(90, 0)
    session = str(uuid.uuid4())[:8]
    clickstream.append({
        "event_id": str(uuid.uuid4()),
        "session_id": f"SES-{session}",
        "customer_id": random.choice(customers)["customer_id"] if random.random() > 0.3 else None,
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "page_url": f"/products/{random.choice(products)['product_id']}",
        "referrer": random.choice(["google", "direct", "social", "email", None]),
        "user_agent": random.choice(DEVICES),
    })

# Write as JSON files (batches of 50,000)
for batch_idx in range(0, len(clickstream), 50000):
    batch = clickstream[batch_idx:batch_idx+50000]
    batch_num = batch_idx // 50000
    path = f"{VOLUME_PATH}/clickstream/batch_{batch_num}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(e) for e in batch), overwrite=True)

print(f"✅ Generated {len(clickstream)} clickstream events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("📦 SAMPLE DATA GENERATION COMPLETE")
print("=" * 60)
print(f"  Customers:   {len(customers):>10,}")
print(f"  Products:    {len(products):>10,}")
print(f"  Orders:      {len(orders):>10,}")
print(f"  Clickstream: {len(clickstream):>10,}")
print(f"  Total:       {sum([len(customers), len(products), len(orders), len(clickstream)]):>10,}")
print("=" * 60)
print(f"  Location: {VOLUME_PATH}")
print(f"  Catalog:  {CATALOG}")
print("=" * 60)
