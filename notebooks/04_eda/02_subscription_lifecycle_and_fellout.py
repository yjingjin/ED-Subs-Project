# Databricks notebook source
# 02 — Subscription Lifecycle & Fell-Out Analysis
#
# Part A — Subscription Lifecycle:
#   Funnel: Intent → Rx Written → Activated/Paid
#   Timing at each stage
#   Overall cancellation pattern
#
# Part B — Fell-Out Analysis:
#   Who never activated? Characteristics and volume
#   What happened before they fell out (Rx written? orders placed?)
#   Comparison: fell-out vs activated on observable signals

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"

QUAL      = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
SUBS_B    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscriptions"
TERMS_B   = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"
INVOICES  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
EVENTS    = f"{CATALOG}.{SCHEMA}.ed_silver_subs_kafka__events"
ORDERS    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_orders"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Part A — Subscription Lifecycle

# COMMAND ----------
# MAGIC %md ## 1. Funnel: Intent → Rx Written → Activated / Paid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT q.subscription_id)                                           AS intent,
# MAGIC     -- Rx written: condition_subscription_prescription_written event
# MAGIC     COUNT(DISTINCT e.subscription_id)                                           AS rx_written,
# MAGIC     ROUND(COUNT(DISTINCT e.subscription_id) * 100.0
# MAGIC           / COUNT(DISTINCT q.subscription_id), 1)                               AS pct_rx_written,
# MAGIC     -- Activated
# MAGIC     SUM(CASE WHEN s.is_activated = TRUE THEN 1 ELSE 0 END)                      AS activated,
# MAGIC     ROUND(SUM(CASE WHEN s.is_activated = TRUE THEN 1.0 ELSE 0 END)
# MAGIC           / COUNT(DISTINCT q.subscription_id) * 100, 1)                         AS pct_activated,
# MAGIC     -- Paid
# MAGIC     SUM(CASE WHEN s.is_paid = TRUE THEN 1 ELSE 0 END)                           AS paid,
# MAGIC     ROUND(SUM(CASE WHEN s.is_paid = TRUE THEN 1.0 ELSE 0 END)
# MAGIC           / COUNT(DISTINCT q.subscription_id) * 100, 1)                         AS pct_paid
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC ) e ON q.subscription_id = e.subscription_id

# COMMAND ----------
# MAGIC %md ## 2. Time from subscription creation to Rx written (days)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, s.created_at::date, e.occurred_at::date) AS days_to_rx,
# MAGIC     COUNT(*)                                                AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC JOIN (
# MAGIC     SELECT subscription_id, MIN(occurred_at) AS occurred_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC     GROUP BY 1
# MAGIC ) e ON q.subscription_id = e.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 3. Time from Rx written to activation (days)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, e.occurred_at::date, s.activated_at::date) AS days_rx_to_activation,
# MAGIC     COUNT(*)                                                  AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC     AND s.is_activated = TRUE
# MAGIC JOIN (
# MAGIC     SELECT subscription_id, MIN(occurred_at) AS occurred_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC     GROUP BY 1
# MAGIC ) e ON q.subscription_id = e.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 4. Overall cancellation pattern — time from activation to cancel request (days)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, s.activated_at::date, t.cancel_requested_at::date) AS days_to_cancel,
# MAGIC     COUNT(*)                                                           AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC WHERE t.cancel_requested_at IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Part B — Fell-Out Analysis

# COMMAND ----------
# MAGIC %md ## 5. Fell-out vs activated volume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN s.is_activated = TRUE THEN 'activated'
# MAGIC         WHEN s.is_paid = FALSE AND s.rx_is_written = FALSE THEN 'fell_out_before_rx'
# MAGIC         WHEN s.rx_is_written = TRUE AND s.is_activated = FALSE THEN 'rx_written_not_activated'
# MAGIC         ELSE 'other'
# MAGIC     END AS lifecycle_stage,
# MAGIC     COUNT(*)                                                AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 6. Fell-out subscribers — plan and drug mix

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     pt.drug_strength,
# MAGIC     pt.regimen,
# MAGIC     pt.term_months,
# MAGIC     COUNT(DISTINCT q.subscription_id)  AS n_fell_out
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC     AND s.is_activated = FALSE
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_plan_terms pt
# MAGIC     ON q.subscription_id = pt.subscription_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2, 3, 4
# MAGIC ORDER BY 5 DESC

# COMMAND ----------
# MAGIC %md ## 7. Fell-out subscribers — did they have an Rx written?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH fell_out AS (
# MAGIC     SELECT q.subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC     JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC         ON q.subscription_id = s.subscription_id
# MAGIC         AND s.is_activated = FALSE
# MAGIC ),
# MAGIC rx_events AS (
# MAGIC     SELECT DISTINCT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE WHEN r.subscription_id IS NOT NULL THEN 'rx_written' ELSE 'no_rx' END AS rx_status,
# MAGIC     COUNT(*)                                                AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct
# MAGIC FROM fell_out f
# MAGIC LEFT JOIN rx_events r ON f.subscription_id = r.subscription_id
# MAGIC GROUP BY 1

# COMMAND ----------
# MAGIC %md ## 8. Fell-out subscribers — did they have an order placed?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH fell_out AS (
# MAGIC     SELECT q.subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC     JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC         ON q.subscription_id = s.subscription_id
# MAGIC         AND s.is_activated = FALSE
# MAGIC ),
# MAGIC orders AS (
# MAGIC     SELECT DISTINCT inv.subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscription_invoices inv
# MAGIC     WHERE inv.subscription_id IN (SELECT subscription_id FROM fell_out)
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE WHEN o.subscription_id IS NOT NULL THEN 'has_order' ELSE 'no_order' END AS order_status,
# MAGIC     COUNT(*)                                                AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct
# MAGIC FROM fell_out f
# MAGIC LEFT JOIN orders o ON f.subscription_id = o.subscription_id
# MAGIC GROUP BY 1

# COMMAND ----------
# MAGIC %md ## 9. Comparison: fell-out vs activated — acquisition channel and platform

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE WHEN s.is_activated = TRUE THEN 'activated' ELSE 'fell_out' END AS group,
# MAGIC     s.first_platform,
# MAGIC     s.first_channel_grouping,
# MAGIC     COUNT(*)                                                AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
# MAGIC         PARTITION BY CASE WHEN s.is_activated = TRUE THEN 'activated' ELSE 'fell_out' END
# MAGIC     ), 1)                                                   AS pct_within_group
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, 4 DESC
