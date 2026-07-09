# Databricks notebook source
# Build all silver tables from bronze.
# Silver = deduped, cleaned, cohort-qualified.
# Re-run is safe: all cells use CREATE OR REPLACE TABLE.
#
# Run order matters:
#   1. subscription_terms_qualified  (base cohort pool — must be first)
#   2. subscription_plan_types  (reference table — no cohort dependency)
#   3-8. All other tables       (filter to qualified subscription_ids)
#
# Cohort definition (Milestone 1):
#   All non-reactivated subscription terms started before 2026-06-01 (data cutoff).
#   No fixed prediction point — rolling window logic applies per-observation filters on top.
#   Reactivated terms are excluded (modeled separately in future milestones).
#
# Two modeling approaches to be tested (see key_definitions.md):
#   Forward:  prediction points roll forward from term_started_at every 30 days until 2026-05-31.
#   Backward: for churned subscribers, prediction points roll backward from cancel_requested_at.

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

# MAGIC %md
# MAGIC Qualified subscription terms are the **first terms** of subscriptions that:
# MAGIC
# MAGIC - were **activated**, and
# MAGIC
# MAGIC - **started before June 1, 2026**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Base cohort pool: all non-reactivated subscription terms started before the data cutoff.
# MAGIC -- No fixed prediction point date — the rolling window logic applies per-observation filters on top.
# MAGIC -- Data cutoff: 2026-06-01 (data was pulled 2026-06-30; latest prediction point is 2026-05-31
# MAGIC --              to allow a full 30-day label window within the pull date).
# MAGIC -- Excludes reactivated terms (subscriber had a previous term that fully ended before this one).
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified AS
# MAGIC SELECT
# MAGIC     DISTINCT t.subscription_id,
# MAGIC     t.subscription_term_id
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_terms` t
# MAGIC JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscriptions` s
# MAGIC on s.subscription_id = t.subscription_id
# MAGIC WHERE t.term_started_at < '2026-06-01'
# MAGIC   AND t.term_number = 1
# MAGIC   AND s.is_paid = TRUE
# MAGIC   AND s.is_activated = TRUE
# MAGIC

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
# MAGIC -- ed_bronze_subscriptions is pre-filtered to is_paid = TRUE AND is_activated = TRUE at export.
# MAGIC -- No additional activation filter needed here — all rows are paid+activated ED subscriptions.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions AS
# MAGIC SELECT
# MAGIC     subs.subscription_id,
# MAGIC     subs.common_id,
# MAGIC     subs.activated_at AS subs_activated_at,
# MAGIC     subs.status,
# MAGIC     subs.is_paid AS subs_is_paid,
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

# MAGIC %md ## 4. subscription_all_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_all_terms AS
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
# MAGIC     terms.cancel_reason,
# MAGIC     CASE
# MAGIC         WHEN terms.cancel_requested_at < '2026-07-01' THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_cancelled_before_cutoff,
# MAGIC     CASE
# MAGIC         WHEN terms.cancel_requested_at < '2026-07-01' THEN 'cancelled'
# MAGIC         ELSE 'not_cancelled'
# MAGIC     END AS cancel_status_before_cutoff
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
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_charges AS
# MAGIC SELECT
# MAGIC     charges.charge_id,
# MAGIC     charges.invoice_id,
# MAGIC     charges.subscription_id,
# MAGIC     charges.occurred_at,
# MAGIC     charges.attempt_number,
# MAGIC     charges.is_latest_charge,
# MAGIC     charges.amount_due,
# MAGIC     charges.gross_revenue,
# MAGIC     charges.net_revenue,
# MAGIC     charges.is_paid,
# MAGIC     charges.is_failed,
# MAGIC     charges.was_paid,
# MAGIC     charges.is_refunded,
# MAGIC     charges.refunded_at,
# MAGIC     charges.failure_reason,
# MAGIC     charges.sub_state_after_charge,
# MAGIC     charges.card_brand,
# MAGIC     charges.event_name
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_charges` AS charges
# MAGIC WHERE charges.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY charges.subscription_id, charges.occurred_at

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
# MAGIC     invoices.visit_id,
# MAGIC     invoices.prescription_id,
# MAGIC     invoices.plan_id,
# MAGIC     invoices.drug_id,
# MAGIC     invoices.drug_name,
# MAGIC     invoices.term_started_at,
# MAGIC     invoices.term_ended_at,
# MAGIC     invoices.cancel_requested_at,
# MAGIC     DATEADD(DAY, plan_types.term_months * 30, invoices.created_at) AS expected_refill_dt,
# MAGIC     invoices.next_invoice_number,
# MAGIC     invoices.next_invoice_status
# MAGIC FROM `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_invoices` AS invoices
# MAGIC LEFT JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_plan_types` AS plan_types
# MAGIC     ON invoices.plan_id = plan_types.plan_id
# MAGIC WHERE invoices.subscription_term_id IN (
# MAGIC     SELECT subscription_term_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY invoices.subscription_id, invoices.created_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. subs_kafka__events

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events AS
# MAGIC SELECT
# MAGIC     e.event_id,
# MAGIC     e.event_name,
# MAGIC     e.subscription_id,
# MAGIC     e.common_id,
# MAGIC     e.raw_occurred_at,
# MAGIC     e.occurred_at,
# MAGIC     e.old_renewal_at,
# MAGIC     e.new_renewal_at,
# MAGIC     e.current_term_end_at,
# MAGIC     e.subscription_created_at,
# MAGIC     e.changed_by,
# MAGIC     e.reason,
# MAGIC     e.state,
# MAGIC     e.raw_state,
# MAGIC     e.termination_type,
# MAGIC     e.condition_id,
# MAGIC     e.condition_name,
# MAGIC     e.drug_id,
# MAGIC     e.drug_name,
# MAGIC     e.drug_strength,
# MAGIC     e.monthly_dose,
# MAGIC     e.plan_id,
# MAGIC     e.plan_name,
# MAGIC     e.new_plan_id,
# MAGIC     e.old_plan_id,
# MAGIC     e.amount_due,
# MAGIC     e.attempt_number,
# MAGIC     e.is_succeeded,
# MAGIC     e.is_failed,
# MAGIC     e.is_charge,
# MAGIC     e.is_refund,
# MAGIC     e.failure_reason,
# MAGIC     e.cancel_at_current_term_end,
# MAGIC     e.invoice_id,
# MAGIC     e.order_id
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events e
# MAGIC WHERE e.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC )
# MAGIC ORDER BY e.subscription_id, e.raw_occurred_at

# COMMAND ----------

# MAGIC %md ## 9. current_periods (deprecated — no longer used)
# MAGIC -- current_periods was built for the invoice-based cohort (Milestone 1 v1).
# MAGIC -- It is superseded by the terms-based cohort in subscriptions_qualified.
# MAGIC -- Kept here for reference; do not run.

# COMMAND ----------

# MAGIC %md ## 10. subscription_term_start_labels (cancellation label)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancellation label per qualified, ACTIVATED subscription term.
# MAGIC -- Prediction point: term_started_at (day 0 of each term).
# MAGIC -- Label window: term_started_at to term_started_at + 29 days (30 days inclusive).
# MAGIC -- Activated subs only: bronze subscriptions is NOT pre-filtered by activation,
# MAGIC -- so filter is applied explicitly here via subs_is_activated = TRUE.
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels AS
# MAGIC SELECT
# MAGIC     q.subscription_id,
# MAGIC     q.subscription_term_id,
# MAGIC     t.term_started_at::date AS term_started_at,
# MAGIC     DATEADD(DAY, 1, t.term_started_at::date)  AS day_1_after_start,
# MAGIC     DATEADD(DAY, 30, t.term_started_at::date) AS day_30_after_start,  
# MAGIC     t.cancel_requested_at::date AS cancel_requested_at,
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at::date BETWEEN t.term_started_at::date AND DATEADD(DAY, 30, t.term_started_at::date)
# MAGIC         THEN 1 ELSE 0
# MAGIC     END AS is_cancelled,
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at::date BETWEEN DATEADD(DAY, 1, t.term_started_at::date) AND DATEADD(DAY, 30, t.term_started_at::date) THEN 'cancelled_in_30_days'
# MAGIC         WHEN t.cancel_requested_at::date = t.term_started_at::date THEN 'cancelled_at_start'
# MAGIC         ELSE 'not_cancelled'
# MAGIC     END AS cancel_status
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN `general_scratch_catalog`.`general_scratch`.`ed_bronze_subscription_terms` t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id

# COMMAND ----------

# MAGIC %md ## 11. subscriptions_churn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: replace with your SQL
# MAGIC -- Filter to qualified subscription_ids:
# MAGIC --   WHERE subscription_id IN (SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified)
# MAGIC -- CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscriptions_churn AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md ## 11. subscription_weekly_snapshots (rolling window base table)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (subscription, snapshot_date) using weekly steps from term_started_at.
# MAGIC -- Cohort: ed_silver_subscription_terms_qualified (activated, non-reactivated, first-term).
# MAGIC -- Stop rules:
# MAGIC --   Churners:     stop when snapshot_date >= cancel_requested_at
# MAGIC --   Non-churners: stop when snapshot_date > '2026-05-31'
# MAGIC -- Label (consistent with modeling label):
# MAGIC --   label = 1 if cancel_requested_at BETWEEN snapshot_date + 1 AND snapshot_date + 30
# MAGIC --   label = 0 otherwise
# MAGIC -- Feature window (for feature engineering): [snapshot_date - N, snapshot_date] inclusive
# MAGIC -- Label window:                             [snapshot_date + 1, snapshot_date + 30] inclusive
# MAGIC CREATE OR REPLACE TABLE general_scratch_catalog.general_scratch.ed_silver_subscription_weekly_snapshots AS
# MAGIC WITH qualified AS (
# MAGIC     SELECT
# MAGIC         q.subscription_id,
# MAGIC         q.subscription_term_id,
# MAGIC         t.term_started_at::date     AS term_started_at,
# MAGIC         -- Cap cancel_requested_at at label cutoff (2026-06-30).
# MAGIC         -- Data was pulled on 2026-07-02 so cancellations on 7/1 and 7/2 exist.
# MAGIC         -- These are treated as not cancelled (label = 0) for all snapshots
# MAGIC         -- whose label window ends on or before 2026-06-30.
# MAGIC         CASE
# MAGIC             WHEN t.cancel_requested_at::date <= DATE '2026-06-30'
# MAGIC             THEN t.cancel_requested_at::date
# MAGIC             ELSE NULL
# MAGIC         END AS cancel_requested_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC     JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC         ON q.subscription_term_id = t.subscription_term_id
# MAGIC ),
# MAGIC snapshots AS (
# MAGIC     SELECT
# MAGIC         q.subscription_id,
# MAGIC         q.subscription_term_id,
# MAGIC         q.term_started_at,
# MAGIC         q.cancel_requested_at,
# MAGIC         DATEADD(DAY, w.week_num * 7, q.term_started_at) AS snapshot_date,
# MAGIC         w.week_num                                        AS week_number,
# MAGIC         w.week_num * 7                                    AS days_since_term_start
# MAGIC     FROM qualified q
# MAGIC     JOIN (SELECT explode(sequence(0, 52)) AS week_num) w ON TRUE
# MAGIC     WHERE
# MAGIC         -- Churners: generate snapshots strictly before cancellation date
# MAGIC         (q.cancel_requested_at IS NOT NULL
# MAGIC          AND DATEADD(DAY, w.week_num * 7, q.term_started_at) < q.cancel_requested_at)
# MAGIC         OR
# MAGIC         -- Non-churners: generate snapshots where label window fits within label cutoff (2026-06-30)
# MAGIC         (q.cancel_requested_at IS NULL
# MAGIC          AND DATEADD(DAY, w.week_num * 7 + 30, q.term_started_at) <= DATE '2026-06-30')
# MAGIC )
# MAGIC SELECT
# MAGIC     subscription_id,
# MAGIC     subscription_term_id,
# MAGIC     term_started_at,
# MAGIC     cancel_requested_at,
# MAGIC     snapshot_date,
# MAGIC     week_number,
# MAGIC     days_since_term_start,
# MAGIC     CASE
# MAGIC         WHEN cancel_requested_at BETWEEN DATEADD(DAY, 1, snapshot_date)
# MAGIC                                      AND DATEADD(DAY, 30, snapshot_date)
# MAGIC         THEN 1 ELSE 0
# MAGIC     END AS label
# MAGIC FROM snapshots

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Row count summary — run after all tables are built
silver_tables = [
    "subscription_terms_qualified",
    "subscription_plan_types",
    "subscriptions",
    "subscription_all_terms",
    "subscription_plan_terms",
    "subscription_charges",
    "subscription_invoices",
    "subs_kafka__events",
    # "current_periods",    # deprecated
    "subscription_term_start_labels",
    "subscription_weekly_snapshots",
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
