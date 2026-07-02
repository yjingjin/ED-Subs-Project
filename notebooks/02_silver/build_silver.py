# Databricks notebook source
# Build all silver tables from bronze.
# Silver = deduped, cleaned, cohort-qualified.
# Re-run is safe: all cells use CREATE OR REPLACE TABLE.
#
# Run order matters:
#   1. subscription_terms_qualified  (defines the cohort — must be first)
#   2. subscription_plan_types  (reference table — no cohort dependency)
#   3-8. All other tables       (filter to qualified subscription_ids)
#
# Cohort definition (Milestone 1):
#   Active subscriptions with no prior cancellation request as of 2026-05-01.
#   Snapshot date: 2026-05-01. Label window: 2026-05-01 to 2026-05-31.
#   Label: did the subscriber request cancellation within 30 days of snapshot?

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"
B       = f"{CATALOG}.{SCHEMA}.ed_bronze_"   # bronze prefix
S       = f"{CATALOG}.{SCHEMA}.ed_silver_"   # silver prefix

print(f"Bronze : {B}*")
print(f"Silver : {S}*")

# COMMAND ----------

# MAGIC %md ## 1. subscription_terms_qualified (cohort — run first)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cohort: active subscriptions with no prior cancellation request as of 2026-05-01.
# MAGIC -- Snapshot date: 2026-05-01
# MAGIC -- Source: ed_bronze_subscription_terms (actual term dates, not computed)
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified AS
# MAGIC SELECT
# MAGIC     DISTINCT subscription_id,
# MAGIC     subscription_term_id
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_terms`
# MAGIC WHERE term_started_at <= '2026-05-01'
# MAGIC   AND (term_ended_at IS NULL OR term_ended_at > '2026-05-01')
# MAGIC   AND (cancel_requested_at IS NULL OR cancel_requested_at > '2026-05-01')

# COMMAND ----------

# MAGIC %md ## 2. subscription_plan_types (reference table)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_plan_types AS
# MAGIC SELECT * FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_types`

# COMMAND ----------

# MAGIC %md ## 3. subscriptions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions AS
# MAGIC SELECT
# MAGIC     subs.subscription_id,
# MAGIC     subs.common_id,
# MAGIC     subs.activated_at AS subs_activated_at,
# MAGIC     subs.status,
# MAGIC     subs.is_activated AS subs_is_activated,
# MAGIC     subs.latest_canceled_at AS subs_latest_canceled_at,
# MAGIC     subs.latest_paused_at AS subs_latest_paused_at,
# MAGIC     subs.user_subscription_number,
# MAGIC     subs.first_platform,
# MAGIC     subs.first_channel_grouping,
# MAGIC     subs.first_traffic_source,
# MAGIC     subs.first_campaign,
# MAGIC     subs.user_state
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscriptions` AS subs
# MAGIC WHERE subs.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY subs_activated_at

# COMMAND ----------

# MAGIC %md ## 4. subscription_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_terms AS
# MAGIC SELECT
# MAGIC     terms.subscription_id,
# MAGIC     terms.subscription_term_id,
# MAGIC     terms.term_number,
# MAGIC     terms.term_started_at,
# MAGIC     terms.term_ended_at,
# MAGIC     terms.term_active_until,
# MAGIC     DATEDIFF(DAY, terms.term_started_at::date, terms.term_ended_at::date) AS days_between_term_start_and_end,
# MAGIC     terms.term_status,
# MAGIC     terms.termination_type,
# MAGIC     terms.is_new_start,
# MAGIC     terms.is_paid AS term_is_paid,
# MAGIC     terms.paid_at AS term_paid_at,
# MAGIC     terms.is_delinquent,
# MAGIC     terms.is_failed_payment_canceled,
# MAGIC     terms.is_refunded,
# MAGIC     terms.refunded_at,
# MAGIC     terms.cancel_requested_at,
# MAGIC     terms.cancel_reason
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_terms` AS terms
# MAGIC WHERE terms.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY terms.subscription_id, terms.term_started_at

# COMMAND ----------

# MAGIC %md ## 5. subscription_plan_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_plan_terms AS
# MAGIC SELECT
# MAGIC     plan_terms.subscription_id,
# MAGIC     plan_terms.subscription_term_id,
# MAGIC     plan_terms.subscription_plan_term_id,
# MAGIC     plan_terms.term_number,
# MAGIC     plan_terms.plan_term_number,
# MAGIC     plan_terms.term_started_at,
# MAGIC     plan_terms.term_ended_at,
# MAGIC     plan_terms.term_status,
# MAGIC     plan_terms.plan_term_started_at,
# MAGIC     plan_terms.plan_term_ended_at,
# MAGIC     plan_terms.plan_term_status,
# MAGIC     plan_terms.is_latest_plan_term,
# MAGIC     plan_terms.plan_id,
# MAGIC     plan_terms.previous_plan_id,
# MAGIC     plan_terms.plan_name,
# MAGIC     plan_terms.plan_variant,
# MAGIC     plan_terms.term_months,
# MAGIC     plan_terms.drug_id,
# MAGIC     plan_terms.drug_name,
# MAGIC     plan_terms.drug_strength,
# MAGIC     plan_terms.regimen,
# MAGIC     plan_terms.monthly_dose,
# MAGIC     plan_terms.starting_fulfillment_type
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_terms` AS plan_terms
# MAGIC WHERE plan_terms.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY plan_terms.subscription_id, plan_terms.plan_term_started_at

# COMMAND ----------

# MAGIC %md ## 6. subscription_charges

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: replace with your SQL
# MAGIC -- Filter to qualified subscription_ids:
# MAGIC --   WHERE subscription_id IN (SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified)
# MAGIC -- CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_charges AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md ## 7. subscription_invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_invoices AS
# MAGIC SELECT
# MAGIC     invoices.subscription_id,
# MAGIC     invoices.subscription_term_id,
# MAGIC     invoices.subscription_plan_term_id,
# MAGIC     invoices.invoice_id,
# MAGIC     invoices.invoice_number,
# MAGIC     invoices.term_number,
# MAGIC     invoices.invoice_status,
# MAGIC     invoices.created_at,
# MAGIC     invoices.paid_at,
# MAGIC     invoices.failed_at,
# MAGIC     invoices.first_charge_at,
# MAGIC     invoices.latest_charge_at,
# MAGIC     invoices.refunded_at,
# MAGIC     invoices.num_charges,
# MAGIC     invoices.amount_due,
# MAGIC     invoices.gross_revenue,
# MAGIC     invoices.net_revenue,
# MAGIC     invoices.is_paid,
# MAGIC     invoices.is_failed,
# MAGIC     invoices.is_refunded,
# MAGIC     invoices.is_delinquent,
# MAGIC     invoices.caused_cancellation,
# MAGIC     invoices.is_latest_term_invoice,
# MAGIC     invoices.plan_id,
# MAGIC     invoices.drug_id,
# MAGIC     invoices.drug_name,
# MAGIC     invoices.term_started_at,
# MAGIC     invoices.term_ended_at,
# MAGIC     invoices.expected_term_end_at,
# MAGIC     invoices.cancel_requested_at,
# MAGIC     DATEADD(DAY, plan_types.term_months * 30, invoices.created_at) AS expected_refill_dt,
# MAGIC     invoices.next_invoice_number,
# MAGIC     invoices.next_invoice_status
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_invoices` AS invoices
# MAGIC LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_types` AS plan_types
# MAGIC     ON invoices.plan_id = plan_types.plan_id
# MAGIC WHERE invoices.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY invoices.subscription_id, invoices.created_at

# COMMAND ----------

# MAGIC %md ## 8. current_periods (deprecated — no longer used)
# MAGIC -- current_periods was built for the invoice-based cohort (Milestone 1 v1).
# MAGIC -- It is superseded by the terms-based cohort in subscriptions_qualified.
# MAGIC -- Kept here for reference; do not run.

# COMMAND ----------

# MAGIC %md ## 9. subscription_labels (cancellation label)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancellation label per qualified subscribtion.
# MAGIC -- Snapshot date: 2026-05-01. Label window: 2026-05-01 (exclusive) to 2026-05-31 (inclusive) (30 days).
# MAGIC -- is_cancelled = 1: cancel_requested_at falls within the label window.
# MAGIC -- is_cancelled = 0: no cancellation request in the label window.
# MAGIC -- Voluntary vs involuntary churn distinction to be added in a future milestone.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_labels AS
# MAGIC SELECT
# MAGIC     q.subscription_id,
# MAGIC     q.subscription_term_id,
# MAGIC     t.term_started_at,
# MAGIC     t.term_ended_at,
# MAGIC     t.term_active_until,
# MAGIC     t.term_status,
# MAGIC     t.cancel_requested_at,
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at IS NOT NULL
# MAGIC          AND t.cancel_requested_at <= '2026-05-31'
# MAGIC         THEN 1 ELSE 0
# MAGIC     END AS is_cancelled,
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at IS NOT NULL
# MAGIC          AND t.cancel_requested_at <= '2026-05-31'
# MAGIC         THEN 'cancelled' ELSE 'not_cancelled'
# MAGIC     END AS cancel_status
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_terms` t
# MAGIC     ON q.subscription_id = t.subscription_id
# MAGIC    AND q.subscription_term_id = t.subscription_term_id

# COMMAND ----------

# MAGIC %md ## 10. subscriptions_churn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: replace with your SQL
# MAGIC -- Filter to qualified subscription_ids:
# MAGIC --   WHERE subscription_id IN (SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified)
# MAGIC -- CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions_churn AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Row count summary — run after all tables are built
silver_tables = [
    "subscription_terms_qualified",
    "subscription_plan_types",
    "subscriptions",
    "subscription_terms",
    "subscription_plan_terms",
    # "subscription_charges",
    "subscription_invoices",
    # "current_periods",    # deprecated
    "subscription_labels",
    # "subscriptions_churn",
]

print(f"\n{'Table':<70} {'Rows':>10}")
print("-" * 82)
for t in silver_tables:
    target = f"{S}{t}"
    try:
        n = spark.table(target).count()
        print(f"{target:<70} {n:>10,}")
    except Exception as e:
        print(f"{target:<70} {'ERROR':>10}  ({e})")
