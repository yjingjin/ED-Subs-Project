# Databricks notebook source
# Load CSVs from the Databricks Volume into bronze Delta tables in general_scratch.
# Run this notebook once per ingestion (re-run is safe: uses overwrite mode).
#
# All tables land in: general_scratch_catalog.general_scratch
# with the prefix "ed_bronze_" to distinguish from silver/gold tables.
#
# Prerequisites:
#   1. CSV files uploaded to VOLUME_BASE (below).
#   2. Schema general_scratch_catalog.general_scratch already exists (it does).

# COMMAND ----------

CATALOG       = "general_scratch_catalog"
SCHEMA        = "general_scratch"
BRONZE_PREFIX = "ed_bronze_"
VOLUME_BASE   = "/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads"

# (csv_filename_stem, table_name_suffix)  →  final table: CATALOG.SCHEMA.BRONZE_PREFIX + suffix
TABLES = [
    ("subscriptions",           "subscriptions"),
    ("subscription_terms",      "subscription_terms"),
    ("subscription_plan_terms", "subscription_plan_terms"),
    ("subscription_charges",    "subscription_charges"),
    ("subscription_invoices",   "subscription_invoices"),
    ("subscriptions_churn",     "subscriptions_churn"),
]

# COMMAND ----------

from pyspark.sql import functions as F

results = []

for csv_stem, table_suffix in TABLES:
    csv_path = f"{VOLUME_BASE}/{csv_stem}.csv"
    target   = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}{table_suffix}"

    print(f"\n--- Loading {csv_path}")
    print(f"        →  {target} ---")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # keep everything as strings; type in silver
        .option("multiLine", True)      # handle embedded newlines in text fields
        .option("escape", '"')
        .csv(csv_path)
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(csv_path))
        .withColumn("_source_table", F.lit(f"goodrx_dbt.{csv_stem}"))
    )

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    print(f"  Rows loaded : {row_count:,}")
    results.append({"table": target, "rows": row_count})

# COMMAND ----------

# Summary
print("\n=== Bronze ingestion complete ===")
print(f"{'Table':<80} {'Rows':>10}")
print("-" * 92)
for r in results:
    print(f"{r['table']:<80} {r['rows']:>10,}")

# COMMAND ----------

# Quick validation: re-count from Delta to confirm writes landed.
print("\n=== Delta row counts (confirmation) ===")
for _, table_suffix in TABLES:
    target = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}{table_suffix}"
    n = spark.table(target).count()
    print(f"  {target:<75} {n:>10,}")
