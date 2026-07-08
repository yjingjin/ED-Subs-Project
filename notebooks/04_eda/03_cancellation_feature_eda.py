# Databricks notebook source
# 03 — Cancellation Feature EDA
# Explores which features correlate with cancellation in the first 30 days of a subscription term.
# Cohort: activated, non-reactivated subscribers (ed_silver_subscription_term_start_labels)
# Label: cancel_status — cancelled_in_30_days / cancelled_at_start / not_cancelled
#
# Analysis plan:
#   1.  Overall cancellation rate breakdown
#   2.  Cancellation rate by cadence (term length)
#   3.  Cancellation rate by drug name
#   4.  Cancellation rate by regimen
#   5.  Cancellation rate by drug strength
#   6.  Cancellation rate by monthly dose
#   7.  Cancellation rate by plan change count
#   8.  Cancellation rate by payment failure history
#   9.  Cancellation rate by delinquency
#   10. Cancellation rate by order behavior
#   11. Cancellation rate by acquisition channel
#   12. Cancellation rate by platform
#   13. Cancellation rate by user state (top states)
#   14. Cancellation timing distribution (days to cancel) by cadence

# COMMAND ----------

CATALOG    = "general_scratch_catalog"
SCHEMA     = "general_scratch"

LABELS     = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_term_start_labels"
SUBS       = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
TERMS      = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_all_terms"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"
INVOICES   = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
ORDERS     = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_orders"
TERMS_B    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"

spark.conf.set("eda.labels",     LABELS)
spark.conf.set("eda.subs",       SUBS)
spark.conf.set("eda.terms",      TERMS)
spark.conf.set("eda.plan_terms", PLAN_TERMS)
spark.conf.set("eda.invoices",   INVOICES)
spark.conf.set("eda.orders",     ORDERS)
spark.conf.set("eda.terms_b",    TERMS_B)

# COMMAND ----------
# MAGIC %md ## 1. Overall cancellation rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     COUNT(*)                                                    AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
# MAGIC FROM ${eda.labels}
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 2. Cancellation rate by cadence (term length)
# MAGIC
# MAGIC Shorter plans cancel more frequently — 1-month subscribers have ~2x the cancel rate of 6-month subscribers.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.term_months                                           AS cadence,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 3. Cancellation rate by drug name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 4 DESC

# COMMAND ----------
# MAGIC %md ## 4. Cancellation rate by regimen

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.regimen,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 4 DESC

# COMMAND ----------
# MAGIC %md ## 5. Cancellation rate by drug strength

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     pt.drug_strength,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 5 DESC

# COMMAND ----------
# MAGIC %md ## 6. Cancellation rate by monthly dose

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     pt.monthly_dose,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 5 DESC

# COMMAND ----------
# MAGIC %md ## 7. Cancellation rate by plan change count
# MAGIC
# MAGIC Subscribers who change plans more often tend to have lower cancel rates — they are more engaged.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH plan_change_counts AS (
# MAGIC     SELECT
# MAGIC         subscription_term_id,
# MAGIC         COUNT(*) - 1 AS plan_change_count   -- subtract 1 because first plan is not a change
# MAGIC     FROM ${eda.plan_terms}
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     pc.plan_change_count,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN plan_change_counts pc
# MAGIC     ON l.subscription_term_id = pc.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 8. Cancellation rate by payment failure history

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH invoice_stats AS (
# MAGIC     SELECT
# MAGIC         subscription_term_id,
# MAGIC         COUNT(*) AS total_invoices,
# MAGIC         SUM(CASE WHEN is_failed = TRUE THEN 1 ELSE 0 END) AS failed_invoice_count,
# MAGIC         SUM(CASE WHEN is_paid = TRUE THEN gross_revenue ELSE 0 END) AS total_paid_amount,
# MAGIC         COUNT(CASE WHEN is_paid = TRUE THEN 1 END) AS count_paid_invoices
# MAGIC     FROM ${eda.invoices}
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN ist.failed_invoice_count = 0 THEN '0 failures'
# MAGIC         WHEN ist.failed_invoice_count = 1 THEN '1 failure'
# MAGIC         WHEN ist.failed_invoice_count BETWEEN 2 AND 3 THEN '2-3 failures'
# MAGIC         ELSE '4+ failures'
# MAGIC     END AS payment_failure_group,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN invoice_stats ist
# MAGIC     ON l.subscription_term_id = ist.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 9. Cancellation rate by delinquency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_delinquent,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.terms} t
# MAGIC     ON l.subscription_term_id = t.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 10. Cancellation rate by order count (fulfillment behavior)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_counts AS (
# MAGIC     SELECT
# MAGIC         inv.subscription_term_id,
# MAGIC         COUNT(DISTINCT ord.order_id) AS order_count
# MAGIC     FROM ${eda.invoices} inv
# MAGIC     LEFT JOIN ${eda.orders} ord
# MAGIC         ON inv.invoice_id = ord.order_id
# MAGIC         -- note: orders join via latest_order_id from invoices; adjust if needed
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN oc.order_count = 0 THEN '0 orders'
# MAGIC         WHEN oc.order_count = 1 THEN '1 order'
# MAGIC         WHEN oc.order_count BETWEEN 2 AND 3 THEN '2-3 orders'
# MAGIC         ELSE '4+ orders'
# MAGIC     END AS order_group,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN order_counts oc
# MAGIC     ON l.subscription_term_id = oc.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 11. Cancellation rate by acquisition channel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.first_channel_grouping,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 12. Cancellation rate by platform

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.first_platform,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 13. Cancellation rate by user state (top 15 states)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.user_state,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END)
# MAGIC                                                              AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC HAVING COUNT(DISTINCT l.subscription_id) >= 100
# MAGIC ORDER BY 2 DESC
# MAGIC LIMIT 15

# COMMAND ----------
# MAGIC %md ## 14. Days to cancel distribution by cadence
# MAGIC
# MAGIC 1-month plans cancel on day 0 and day 23 (refill reminder window).
# MAGIC 3-month plans cancel on day 0 and day 83.
# MAGIC 6-month plans cancel on day 0 and day 173.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.term_months                                               AS cadence,
# MAGIC     DATEDIFF(DAY, l.prediction_point, l.cancel_requested_at)    AS days_to_cancel,
# MAGIC     COUNT(*)                                                     AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY pt.term_months), 1) AS pct_within_cadence
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status = 'cancelled_in_30_days'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2
