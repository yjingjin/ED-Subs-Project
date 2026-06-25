# Databricks notebook source
# Load CSVs from the Databricks Volume into bronze Delta tables in general_scratch.
#
# Strategy: read all columns as raw strings first, then cast explicitly per column.
# This is more robust than supplying a schema upfront — Spark's cast() handles mixed
# timestamp formats (with/without timezone offset) and boolean strings cleanly.
# Null counts are printed after each table so you can spot any remaining parse issues.
#
# Re-run is safe: uses overwrite mode.

# COMMAND ----------

CATALOG       = "general_scratch_catalog"
SCHEMA        = "general_scratch"
BRONZE_PREFIX = "ed_bronze_"
VOLUME_BASE   = "/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads"

TABLES = [
    "subscriptions",
    "subscription_terms",
    "subscription_plan_terms",
    "subscription_charges",
    "subscription_invoices",
    "subscriptions_churn",
]

# COMMAND ----------

# Column type maps: table → {column: cast_type}
# Columns not listed stay as StringType.

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

TS  = "timestamp"
BOL = "boolean"
INT = "integer"
DEC = "decimal(18,2)"

CASTS = {}

CASTS["subscriptions"] = {
    "created_at":                       TS,
    "current_term_end":                 TS,
    "latest_delinquent_at":             TS,
    "latest_canceled_at":               TS,
    "latest_paused_at":                 TS,
    "activated_backend_at":             TS,
    "paid_at":                          TS,
    "rx_written_at":                    TS,
    "activated_at":                     TS,
    "trial_converted_at":               TS,
    "trial_ended_at":                   TS,
    "trial_conversion_expected_at":     TS,
    "_kafka_updated_ts":                TS,
    "_updated_ts":                      TS,
    "condition_id":                     INT,
    "trial_duration":                   INT,
    "current_drug_id":                  INT,
    "current_quantity":                 INT,
    "current_days_supply":              INT,
    "desired_refill_count":             INT,
    "user_subscription_number":         INT,
    "current_price":                    DEC,
    "had_trial":                        BOL,
    "is_paid":                          BOL,
    "rx_is_written":                    BOL,
    "is_activated_backend":             BOL,
    "is_trial_converted":               BOL,
    "trial_is_ended":                   BOL,
    "is_manual_trial_canceled":         BOL,
    "is_failed_payment_trial_canceled": BOL,
    "is_trial_canceled":                BOL,
    "is_activated":                     BOL,
}

CASTS["subscription_terms"] = {
    "_sub_activated_at":                TS,
    "term_started_at":                  TS,
    "next_term_started_at":             TS,
    "term_ended_at":                    TS,
    "term_active_until":                TS,
    "trial_converted_at":               TS,
    "trial_ended_at":                   TS,
    "trial_conversion_expected_at":     TS,
    "cancel_requested_at":              TS,
    "cancel_expected_at":               TS,
    "paid_at":                          TS,
    "refunded_at":                      TS,
    "_updated_ts":                      TS,
    "condition_id":                     INT,
    "term_number":                      INT,
    "is_new_start":                     BOL,
    "had_trial":                        BOL,
    "is_trial_converted":               BOL,
    "trial_is_ended":                   BOL,
    "is_trial_canceled":                BOL,
    "is_manual_trial_canceled":         BOL,
    "is_failed_payment_trial_canceled": BOL,
    "is_paid":                          BOL,
    "is_delinquent":                    BOL,
    "is_failed_payment_canceled":       BOL,
    "trial_is_delinquent":              BOL,
    "is_refunded":                      BOL,
}

CASTS["subscription_plan_terms"] = {
    "term_started_at":              TS,
    "term_ended_at":                TS,
    "plan_term_started_at":         TS,
    "plan_term_ended_at":           TS,
    "next_plan_term_started_at":    TS,
    "condition_id":                 INT,
    "term_number":                  INT,
    "plan_term_number":             INT,
    "drug_id":                      INT,
    "monthly_dose":                 INT,
    "term_months":                  INT,
    "is_latest_plan_term":          BOL,
}

CASTS["subscription_charges"] = {
    "occurred_at":          TS,
    "current_term_end_at":  TS,
    "refunded_at":          TS,
    "condition_id":         INT,
    "drug_id":              INT,
    "attempt_number":       INT,
    "activation_count":     INT,
    "amount_due":           DEC,
    "gross_revenue":        DEC,
    "net_revenue":          DEC,
    "is_latest_charge":     BOL,
    "is_refunded":          BOL,
    "is_failed":            BOL,
    "was_paid":             BOL,
    "is_paid":              BOL,
}

CASTS["subscription_invoices"] = {
    "created_at":               TS,
    "paid_at":                  TS,
    "failed_at":                TS,
    "first_charge_at":          TS,
    "refunded_at":              TS,
    "latest_charge_at":         TS,
    "latest_term_end_at":       TS,
    "term_started_at":          TS,
    "term_ended_at":            TS,
    "plan_term_started_at":     TS,
    "plan_term_ended_at":       TS,
    "expected_term_end_at":     TS,
    "order_updated_at":         TS,
    "cancel_requested_at":      TS,
    "next_invoice_created_at":  TS,
    "expected_refill_dt":       TS,
    "invoice_number":           INT,
    "activation_count":         INT,
    "num_charges":              INT,
    "term_number":              INT,
    "drug_id":                  INT,
    "next_invoice_number":      INT,
    "days_between_invoice_and_fulfillment_status": INT,
    "amount_due":               DEC,
    "gross_revenue":            DEC,
    "net_revenue":              DEC,
    "was_paid":                 BOL,
    "was_failed":               BOL,
    "is_paid":                  BOL,
    "is_failed":                BOL,
    "is_refunded":              BOL,
    "is_delinquent":            BOL,
    "caused_cancellation":      BOL,
    "is_trial_invoice":         BOL,
    "is_trial_conversion_invoice": BOL,
    "is_latest_term_invoice":   BOL,
    "is_dup":                   BOL,
    "next_invoice_creation_is_expected": BOL,
}

CASTS["subscriptions_churn"] = {
    "term_started_at":        TS,
    "term_ended_at":          TS,
    "cancel_requested_at":    TS,
    "forecasted_churn_date":  TS,
    "first_invoice_paid_dt":  TS,
    "last_invoice_paid_dt":   TS,
    "term_number":            INT,
    "term_months":            INT,
    "count_paid_invoices":    INT,
    "total_paid_amount":      DEC,
    "net_paid_amount":        DEC,
    "is_reactivated_term":    BOL,
    "is_activated":           BOL,
}

# COMMAND ----------

results = []

for table_name in TABLES:
    csv_path = f"{VOLUME_BASE}/{table_name}.csv"
    target   = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}{table_name}"

    print(f"\n--- {table_name} ---")

    # Step 1: read everything as strings
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(csv_path)
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(csv_path))
        .withColumn("_source_table", F.lit(f"goodrx_dbt.{table_name}"))
    )

    # Step 2: apply explicit casts — empty strings become null naturally
    for col_name, cast_type in CASTS[table_name].items():
        df = df.withColumn(col_name, F.col(col_name).cast(cast_type))

    row_count = df.count()

    # Step 3: write to bronze
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    # Step 4: null check on typed columns
    typed_cols = list(CASTS[table_name].keys())
    null_counts = (
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in typed_cols])
        .collect()[0]
        .asDict()
    )
    non_zero = {c: n for c, n in null_counts.items() if n > 0}

    print(f"  Rows   : {row_count:,}")
    print(f"  Nulls  : {non_zero if non_zero else 'none in typed columns'}")
    results.append({"table": target, "rows": row_count, "nulls": non_zero})

# COMMAND ----------

print("\n=== Bronze ingestion complete ===")
print(f"{'Table':<75} {'Rows':>8}  Columns with nulls")
print("-" * 110)
for r in results:
    nulls = ", ".join(f"{c}={n}" for c, n in r["nulls"].items()) or "—"
    print(f"{r['table']:<75} {r['rows']:>8,}  {nulls}")
