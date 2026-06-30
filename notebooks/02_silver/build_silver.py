# Databricks notebook source
# Build all silver tables from bronze.
# Silver = deduped, cleaned, cohort-qualified.
# Re-run is safe: all cells use CREATE OR REPLACE TABLE.
#
# Run order matters:
#   1. subscriptions_qualified  (defines the cohort — must be first)
#   2. subscription_plan_types  (reference table — no cohort dependency)
#   3-8. All other tables       (filter to qualified subscription_ids)

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"
B       = f"{CATALOG}.{SCHEMA}.ed_bronze_"   # bronze prefix
S       = f"{CATALOG}.{SCHEMA}.ed_silver_"   # silver prefix

print(f"Bronze : {B}*")
print(f"Silver : {S}*")

# COMMAND ----------

# MAGIC %md ## 1. subscriptions_qualified (cohort — run first)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cohort: subscriptions whose first paid, non-duplicate invoice has a first_period_end_dt on or before 2026-05-15.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified AS
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         subscription_id,
# MAGIC         subscription_term_id,
# MAGIC         invoice_id,
# MAGIC         created_at AS first_period_start_at,
# MAGIC         expected_refill_dt AS next_billing_dt,
# MAGIC         DATEADD(DAY, -1, expected_refill_dt) AS first_period_end_dt,
# MAGIC         DATEDIFF(DAY, created_at, expected_refill_dt) AS days_between_start_and_next_billing_dt
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             invoices.subscription_id,
# MAGIC             invoices.subscription_term_id,
# MAGIC             invoices.invoice_id,
# MAGIC             invoices.created_at,
# MAGIC             DATEADD(
# MAGIC                 DAY,
# MAGIC                 plan_types.term_months * 30,
# MAGIC                 invoices.created_at
# MAGIC             ) AS expected_refill_dt,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY invoices.subscription_id
# MAGIC                 ORDER BY invoices.created_at
# MAGIC             ) AS rnk
# MAGIC         FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscriptions` AS subs
# MAGIC         LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_invoices` AS invoices
# MAGIC             ON subs.subscription_id = invoices.subscription_id
# MAGIC         LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_types` AS plan_types
# MAGIC             ON invoices.plan_id = plan_types.plan_id
# MAGIC         WHERE invoices.is_paid = true
# MAGIC             AND invoices.is_dup = false
# MAGIC     ) AS temp_1
# MAGIC     WHERE rnk = 1
# MAGIC     ORDER BY subscription_id, first_period_start_at
# MAGIC ) AS temp_2
# MAGIC WHERE first_period_end_dt::date <= '2026-05-15'

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
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
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
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
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
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC )
# MAGIC ORDER BY plan_terms.subscription_id, plan_terms.plan_term_started_at

# COMMAND ----------

# MAGIC %md ## 6. subscription_charges

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: replace with your SQL
# MAGIC -- Filter to qualified subscription_ids:
# MAGIC --   WHERE subscription_id IN (SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified)
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
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC )
# MAGIC ORDER BY invoices.subscription_id, invoices.created_at

# COMMAND ----------

# MAGIC %md ## 8. current_periods

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current period per subscriber: most recent paid, non-dup period ending on or before 2026-05-15.
# MAGIC -- Used as the observation point for churn labeling.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_current_periods AS
# MAGIC SELECT
# MAGIC     subscription_id,
# MAGIC     subscription_term_id,
# MAGIC     invoice_id,
# MAGIC     current_period_start_at,
# MAGIC     current_period_end_dt,
# MAGIC     renewal_window_end_dt
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY subscription_id
# MAGIC             ORDER BY created_at DESC, invoice_id DESC
# MAGIC         ) AS rnk
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             invoices.subscription_id,
# MAGIC             invoices.subscription_term_id,
# MAGIC             invoices.invoice_id,
# MAGIC             invoices.created_at,
# MAGIC             invoices.created_at::date AS current_period_start_at,
# MAGIC             DATEADD(
# MAGIC                 DAY,
# MAGIC                 plan_types.term_months * 30,
# MAGIC                 invoices.created_at
# MAGIC             ) AS expected_refill_dt,
# MAGIC             DATEADD(
# MAGIC                 DAY,
# MAGIC                 plan_types.term_months * 30 - 1,
# MAGIC                 invoices.created_at
# MAGIC             ) AS current_period_end_dt,
# MAGIC             DATEADD(
# MAGIC                 DAY,
# MAGIC                 plan_types.term_months * 30 - 1 + 30,
# MAGIC                 invoices.created_at
# MAGIC             ) AS renewal_window_end_dt
# MAGIC         FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_invoices` AS invoices
# MAGIC         LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_types` AS plan_types
# MAGIC             ON invoices.plan_id = plan_types.plan_id
# MAGIC         WHERE invoices.subscription_id IN (
# MAGIC             SELECT subscription_id
# MAGIC             FROM `general_scratch_catalog`.`general_scratch`.`ed_silver_subscriptions_qualified`
# MAGIC         )
# MAGIC           AND invoices.is_paid = true
# MAGIC           AND invoices.is_dup = false
# MAGIC           AND DATEADD(
# MAGIC                 DAY,
# MAGIC                 plan_types.term_months * 30 - 1,
# MAGIC                 invoices.created_at
# MAGIC               )::date <= DATE '2026-05-15'
# MAGIC     ) a
# MAGIC ) b
# MAGIC WHERE rnk = 1

# COMMAND ----------

# MAGIC %md ## 9. subscription_labels (churn label)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn label per qualified subscriber based on current period.
# MAGIC -- is_churned = 0 (retained): new paid invoice OR new active term within 30 days after current period end.
# MAGIC -- is_churned = 1 (churned): no renewal within 30 days.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_labels AS
# MAGIC
# MAGIC WITH renew AS (
# MAGIC     -- Finds the next renewal invoice within the 30-day window after the current period.
# MAGIC     -- Lower bound uses current_period_start_at (not current_period_end_dt) because
# MAGIC     -- renewal invoices are often created a few days BEFORE the period technically ends
# MAGIC     -- (the billing system charges early to ensure continuity). Using current_period_end_dt
# MAGIC     -- as the lower bound would miss these early renewals and inflate the churn rate.
# MAGIC     SELECT
# MAGIC         cp.subscription_id,
# MAGIC         MIN(inv.created_at)::date AS next_paid_invoice_created_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_current_periods cp
# MAGIC     JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_invoices inv
# MAGIC       ON cp.subscription_id = inv.subscription_id
# MAGIC      AND cp.subscription_term_id = inv.subscription_term_id
# MAGIC      AND inv.is_paid = TRUE
# MAGIC      AND inv.invoice_id <> cp.invoice_id
# MAGIC      AND inv.created_at::date > cp.current_period_start_at
# MAGIC      AND inv.created_at::date <= cp.renewal_window_end_dt
# MAGIC     GROUP BY 1
# MAGIC ),
# MAGIC
# MAGIC reactivation AS (
# MAGIC     SELECT
# MAGIC         cp.subscription_id,
# MAGIC         MIN(t.term_started_at)::date AS next_active_term_started_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_current_periods cp
# MAGIC     JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_terms t
# MAGIC       ON cp.subscription_id = t.subscription_id
# MAGIC      AND t.subscription_term_id <> cp.subscription_term_id
# MAGIC      AND t.term_started_at::date >= cp.current_period_end_dt
# MAGIC      AND t.term_started_at::date <= cp.renewal_window_end_dt
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     cp.subscription_id,
# MAGIC     cp.subscription_term_id,
# MAGIC     cp.invoice_id,
# MAGIC     cp.current_period_start_at,
# MAGIC     cp.current_period_end_dt,
# MAGIC     cp.renewal_window_end_dt,
# MAGIC     npi.next_paid_invoice_created_at AS auto_renew_dt,
# MAGIC     nat.next_active_term_started_at AS reactivation_dt,
# MAGIC     CASE WHEN npi.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END
# MAGIC         AS is_auto_renewed,
# MAGIC     CASE WHEN nat.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END
# MAGIC         AS is_reactivated,
# MAGIC     CASE
# MAGIC         WHEN npi.subscription_id IS NOT NULL
# MAGIC           OR nat.subscription_id IS NOT NULL
# MAGIC         THEN 0 ELSE 1
# MAGIC     END AS churn_label,
# MAGIC     CASE
# MAGIC         WHEN npi.subscription_id IS NOT NULL
# MAGIC           OR nat.subscription_id IS NOT NULL
# MAGIC         THEN 'retained' ELSE 'churned'
# MAGIC     END AS churn_status
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_current_periods cp
# MAGIC LEFT JOIN renew npi
# MAGIC   ON cp.subscription_id = npi.subscription_id
# MAGIC LEFT JOIN reactivation nat
# MAGIC   ON cp.subscription_id = nat.subscription_id

# COMMAND ----------

# MAGIC %md ## 10. subscriptions_churn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: replace with your SQL
# MAGIC -- Filter to qualified subscription_ids:
# MAGIC --   WHERE subscription_id IN (SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified)
# MAGIC -- CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions_churn AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Row count summary — run after all tables are built
silver_tables = [
    "subscriptions_qualified",
    "subscription_plan_types",
    "subscriptions",
    "subscription_terms",
    "subscription_plan_terms",
    # "subscription_charges",
    "subscription_invoices",
    "current_periods",
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
