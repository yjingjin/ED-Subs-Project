# Databricks notebook source
# Load CSVs from the Databricks Volume into bronze Delta tables in general_scratch.
# Uses explicit schemas per table so all columns have correct types from the start.
# Run this notebook once per ingestion (re-run is safe: uses overwrite mode).

# COMMAND ----------

CATALOG       = "general_scratch_catalog"
SCHEMA        = "general_scratch"
BRONZE_PREFIX = "ed_bronze_"
VOLUME_BASE   = "/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads"

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType,
    DecimalType, TimestampType,
)

T   = TimestampType()
STR = StringType()
INT = IntegerType()
BOL = BooleanType()
DEC = DecimalType(18, 2)

# ---------- per-table schemas ----------

SCHEMAS = {}

SCHEMAS["subscriptions"] = StructType([
    StructField("subscription_id",                  STR),
    StructField("created_at",                       T),
    StructField("common_id",                        STR),
    StructField("status",                           STR),
    StructField("raw_subscription_type",            STR),
    StructField("condition_id",                     INT),
    StructField("tenant_id",                        STR),
    StructField("current_term_end",                 T),
    StructField("latest_delinquent_at",             T),
    StructField("latest_canceled_at",               T),
    StructField("latest_paused_at",                 T),
    StructField("latest_stripe_sub_id",             STR),
    StructField("trial_duration",                   INT),
    StructField("had_trial",                        BOL),
    StructField("current_price",                    DEC),
    StructField("current_drug_id",                  INT),
    StructField("current_regimen",                  STR),
    StructField("current_quantity",                 INT),
    StructField("current_fulfillment_method",       STR),
    StructField("current_days_supply",              INT),
    StructField("desired_refill_count",             INT),
    StructField("current_benefits_package",         STR),
    StructField("current_variant",                  STR),
    StructField("activated_backend_at",             T),
    StructField("paid_at",                          T),
    StructField("rx_written_at",                    T),
    StructField("is_paid",                          BOL),
    StructField("rx_is_written",                    BOL),
    StructField("is_activated_backend",             BOL),
    StructField("is_trial_converted",               BOL),
    StructField("trial_converted_at",               T),
    StructField("trial_is_ended",                   BOL),
    StructField("trial_ended_at",                   T),
    StructField("is_manual_trial_canceled",         BOL),
    StructField("is_failed_payment_trial_canceled", BOL),
    StructField("is_trial_canceled",                BOL),
    StructField("_kafka_updated_ts",                T),
    StructField("subscription_category",            STR),
    StructField("tenant_name",                      STR),
    StructField("subscription_subcategory",         STR),
    StructField("subscription_name",                STR),
    StructField("condition_name",                   STR),
    StructField("is_activated",                     BOL),
    StructField("activated_at",                     T),
    StructField("trial_conversion_expected_at",     T),
    StructField("user_subscription_number",         INT),
    StructField("first_platform",                   STR),
    StructField("first_session_id",                 STR),
    StructField("first_grx_unique_id",              STR),
    StructField("first_channel_grouping",           STR),
    StructField("first_channel_subgrouping",        STR),
    StructField("first_traffic_source",             STR),
    StructField("first_campaign",                   STR),
    StructField("user_state",                       STR),
    StructField("_updated_ts",                      T),
])

SCHEMAS["subscription_terms"] = StructType([
    StructField("common_id",                        STR),
    StructField("subscription_category",            STR),
    StructField("subscription_subcategory",         STR),
    StructField("subscription_name",                STR),
    StructField("condition_name",                   STR),
    StructField("condition_id",                     INT),
    StructField("_sub_activated_at",                T),
    StructField("subscription_id",                  STR),
    StructField("term_started_at",                  T),
    StructField("subscription_term_id",             STR),
    StructField("term_number",                      INT),
    StructField("next_term_started_at",             T),
    StructField("is_new_start",                     BOL),
    StructField("had_trial",                        BOL),
    StructField("is_trial_converted",               BOL),
    StructField("trial_converted_at",               T),
    StructField("trial_is_ended",                   BOL),
    StructField("trial_ended_at",                   T),
    StructField("trial_conversion_expected_at",     T),
    StructField("is_trial_canceled",                BOL),
    StructField("is_manual_trial_canceled",         BOL),
    StructField("is_failed_payment_trial_canceled", BOL),
    StructField("_status",                          STR),
    StructField("term_status",                      STR),
    StructField("first_plan_id",                    STR),
    StructField("latest_plan_id",                   STR),
    StructField("first_active_visit_id",            STR),
    StructField("cancel_requested_at",              T),
    StructField("cancel_expected_at",               T),
    StructField("cancel_reason",                    STR),
    StructField("term_ended_at",                    T),
    StructField("termination_type",                 STR),
    StructField("term_active_until",                T),
    StructField("is_paid",                          BOL),
    StructField("paid_at",                          T),
    StructField("_updated_ts",                      T),
    StructField("is_delinquent",                    BOL),
    StructField("is_failed_payment_canceled",       BOL),
    StructField("trial_is_delinquent",              BOL),
    StructField("refunded_at",                      T),
    StructField("is_refunded",                      BOL),
])

SCHEMAS["subscription_plan_terms"] = StructType([
    StructField("subscription_id",          STR),
    StructField("common_id",                STR),
    StructField("term_started_at",          T),
    StructField("term_ended_at",            T),
    StructField("term_status",              STR),
    StructField("subscription_category",   STR),
    StructField("subscription_subcategory",STR),
    StructField("subscription_name",       STR),
    StructField("condition_name",          STR),
    StructField("condition_id",            INT),
    StructField("subscription_term_id",    STR),
    StructField("term_number",             INT),
    StructField("subscription_plan_term_id", STR),
    StructField("plan_term_started_at",    T),
    StructField("plan_id",                 STR),
    StructField("previous_plan_id",        STR),
    StructField("starting_visit_id",       STR),
    StructField("plan_name",               STR),
    StructField("drug_id",                 INT),
    StructField("drug_name",               STR),
    StructField("drug_strength",           STR),
    StructField("regimen",                 STR),
    StructField("monthly_dose",            INT),
    StructField("plan_variant",            STR),
    StructField("term_months",             INT),
    StructField("plan_term_number",        INT),
    StructField("next_plan_term_started_at", T),
    StructField("next_plan_term_plan_id",  STR),
    StructField("is_latest_plan_term",     BOL),
    StructField("plan_term_ended_at",      T),
    StructField("plan_term_status",        STR),
    StructField("starting_fulfillment_type", STR),
])

SCHEMAS["subscription_charges"] = StructType([
    StructField("charge_id",               STR),
    StructField("event_name",              STR),
    StructField("invoice_id",              STR),
    StructField("is_latest_charge",        BOL),
    StructField("attempt_number",          INT),
    StructField("subscription_id",         STR),
    StructField("occurred_at",             T),
    StructField("common_id",               STR),
    StructField("activation_count",        INT),
    StructField("order_id",                STR),
    StructField("visit_id",                STR),
    StructField("prescription_id",         STR),
    StructField("plan_id",                 STR),
    StructField("plan_name",               STR),
    StructField("subscription_category",   STR),
    StructField("subscription_name",       STR),
    StructField("condition_name",          STR),
    StructField("condition_id",            INT),
    StructField("drug_id",                 INT),
    StructField("drug_name",               STR),
    StructField("failure_reason",          STR),
    StructField("amount_due",              DEC),
    StructField("payment_method_id",       STR),
    StructField("current_term_end_at",     T),
    StructField("sub_state_after_charge",  STR),
    StructField("card_brand",              STR),
    StructField("wallet_type",             STR),
    StructField("is_refunded",             BOL),
    StructField("refunded_at",             T),
    StructField("is_failed",               BOL),
    StructField("was_paid",                BOL),
    StructField("is_paid",                 BOL),
    StructField("status",                  STR),
    StructField("gross_revenue",           DEC),
    StructField("net_revenue",             DEC),
])

SCHEMAS["subscription_invoices"] = StructType([
    StructField("invoice_id",                              STR),
    StructField("created_at",                              T),
    StructField("subscription_id",                         STR),
    StructField("common_id",                               STR),
    StructField("plan_id",                                 STR),
    StructField("original_payment_method_id",              STR),
    StructField("visit_id",                                STR),
    StructField("prescription_id",                         STR),
    StructField("invoice_number",                          INT),
    StructField("card_type",                               STR),
    StructField("was_paid",                                BOL),
    StructField("was_failed",                              BOL),
    StructField("is_paid",                                 BOL),
    StructField("is_failed",                               BOL),
    StructField("is_refunded",                             BOL),
    StructField("invoice_status",                          STR),
    StructField("activation_count",                        INT),
    StructField("paid_at",                                 T),
    StructField("failed_at",                               T),
    StructField("first_charge_at",                         T),
    StructField("refunded_at",                             T),
    StructField("num_charges",                             INT),
    StructField("latest_charge_at",                        T),
    StructField("latest_term_end_at",                      T),
    StructField("latest_charge_id",                        STR),
    StructField("latest_order_id",                         STR),
    StructField("latest_payment_method_id",                STR),
    StructField("amount_due",                              DEC),
    StructField("gross_revenue",                           DEC),
    StructField("net_revenue",                             DEC),
    StructField("is_delinquent",                           BOL),
    StructField("caused_cancellation",                     BOL),
    StructField("subscription_category",                   STR),
    StructField("subscription_subcategory",                STR),
    StructField("subscription_name",                       STR),
    StructField("subscription_term_id",                    STR),
    StructField("term_number",                             INT),
    StructField("term_started_at",                         T),
    StructField("term_ended_at",                           T),
    StructField("is_trial_invoice",                        BOL),
    StructField("is_trial_conversion_invoice",             BOL),
    StructField("subscription_plan_term_id",               STR),
    StructField("plan_term_started_at",                    T),
    StructField("plan_term_ended_at",                      T),
    StructField("drug_id",                                 INT),
    StructField("drug_name",                               STR),
    StructField("expected_term_end_at",                    T),
    StructField("order_status",                            STR),
    StructField("order_updated_at",                        T),
    StructField("cancel_requested_at",                     T),
    StructField("is_latest_term_invoice",                  BOL),
    StructField("is_dup",                                  BOL),
    StructField("days_between_invoice_and_fulfillment_status", INT),
    StructField("next_invoice_number",                     INT),
    StructField("expected_refill_dt",                      T),
    StructField("next_invoice_creation_is_expected",       BOL),
    StructField("next_invoice_created_at",                 T),
    StructField("next_invoice_status",                     STR),
])

SCHEMAS["subscriptions_churn"] = StructType([
    StructField("common_id",              STR),
    StructField("subscription_id",        STR),
    StructField("subscription_term_id",   STR),
    StructField("term_number",            INT),
    StructField("is_reactivated_term",    BOL),
    StructField("subscription_category", STR),
    StructField("subscription_name",      STR),
    StructField("is_activated",           BOL),
    StructField("term_started_at",        T),
    StructField("term_ended_at",          T),
    StructField("first_invoice_paid_dt",  T),
    StructField("last_invoice_paid_dt",   T),
    StructField("count_paid_invoices",    INT),
    StructField("total_paid_amount",      DEC),
    StructField("net_paid_amount",        DEC),
    StructField("plan_name",              STR),
    StructField("term_months",            INT),
    StructField("cancel_requested_at",    T),
    StructField("forecasted_churn_date",  T),
    StructField("user_state",             STR),
])

# COMMAND ----------

from pyspark.sql import functions as F

TABLES = [
    ("subscriptions",           "subscriptions"),
    ("subscription_terms",      "subscription_terms"),
    ("subscription_plan_terms", "subscription_plan_terms"),
    ("subscription_charges",    "subscription_charges"),
    ("subscription_invoices",   "subscription_invoices"),
    ("subscriptions_churn",     "subscriptions_churn"),
]

results = []

for csv_stem, table_suffix in TABLES:
    csv_path = f"{VOLUME_BASE}/{csv_stem}.csv"
    target   = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}{table_suffix}"

    print(f"\n--- Loading {csv_path}")
    print(f"        →  {target} ---")

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .schema(SCHEMAS[csv_stem])
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

print("\n=== Bronze ingestion complete ===")
print(f"{'Table':<80} {'Rows':>10}")
print("-" * 92)
for r in results:
    print(f"{r['table']:<80} {r['rows']:>10,}")

# COMMAND ----------

# Spot-check schemas to confirm types landed correctly.
for _, table_suffix in TABLES:
    target = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}{table_suffix}"
    print(f"\n--- {target} ---")
    spark.table(target).printSchema()
