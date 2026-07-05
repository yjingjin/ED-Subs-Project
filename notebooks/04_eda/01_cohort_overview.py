# Databricks notebook source
# 01 — Cohort Overview
# Validates the cohort and label tables, then provides high-level cohort summary stats.
# Run after build_silver.py.
#
# Sections:
#   Sanity Checks  (1–10): validate cohort and label integrity
#   Overview       (11–14): cohort size over time, label distribution, plan/drug mix, geography

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"
QUAL    = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
LABELS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_term_start_labels"
SUBS    = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
TERMS_B = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"

# COMMAND ----------

# MAGIC %md ## 1. Cohort size

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_terms,
# MAGIC     COUNT(DISTINCT subscription_id) AS unique_subscriptions
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     subscription_id,
# MAGIC     COUNT(DISTINCT subscription_term_id) AS unique_subscriptions
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified
# MAGIC GROUP BY subscription_id HAVING COUNT(DISTINCT subscription_term_id) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from general_scratch_catalog.general_scratch.ed_silver_subscription_all_terms
# MAGIC where subscription_id = '02548d20-74b4-4256-9244-01466dc3c318'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC raw_occurred_at,
# MAGIC occurred_at,
# MAGIC event_name,
# MAGIC old_renewal_at,
# MAGIC new_renewal_at
# MAGIC from general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC where subscription_id = '02548d20-74b4-4256-9244-01466dc3c318'
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC subscription '02548d20-74b4-4256-9244-01466dc3c318' has two terms even though he didn't churn and reactivate. exclude this subscription in final analysis.

# COMMAND ----------

# MAGIC %md ## 2. Term status distribution — most should be active

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.term_status,
# MAGIC     COUNT(*) AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_all_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md ## 3. Cross-tab: cancel_status vs current subscription status

# COMMAND ----------

# MAGIC %md
# MAGIC The cancellation label is assigned within 30 days of the term start. Therefore, we expect no cancelled subscriptions to have an active status. However, as of July 2, 2026, there may be non-cancelled subscriptions that currently have a cancelled status.                

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancelled + active should be near zero (main sanity check)
# MAGIC SELECT
# MAGIC     l.cancel_status,
# MAGIC     s.status,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels l
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Why are some subscriptions labeled "cancelled" but currently "active"?
# MAGIC
# MAGIC **Possible explanation 1:** These subscriptions have been reactivated

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC SELECT
# MAGIC count(distinct l.subscription_id)  n_reactived
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels l
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC join general_scratch_catalog.general_scratch.ed_silver_subscription_all_terms t
# MAGIC on l.subscription_id = t.subscription_id and l.subscription_term_id != t.subscription_term_id
# MAGIC where l.cancel_status = 'cancelled' and s.status = 'active'

# COMMAND ----------

# MAGIC %md
# MAGIC **Possible explanation 2:** Their current term hasn't ended yet.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC count(distinct l.subscription_id)  n_not_ended_yet
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels l
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC join general_scratch_catalog.general_scratch.ed_silver_subscription_all_terms t
# MAGIC     on l.subscription_term_id = t.subscription_term_id
# MAGIC where l.cancel_status = 'cancelled' and s.status = 'active' and t.term_ended_at is null

# COMMAND ----------

# MAGIC %md ## 4. Term start date distribution — confirm all started before 2026-06-01

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', t.term_started_at) AS term_start_month,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 desc

# COMMAND ----------

# MAGIC %md ## 5. Cancellation timing — when did cancelled subscribers cancel?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('week', cancel_requested_at) AS cancel_week,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels
# MAGIC WHERE is_cancelled = 1
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ## 6. Check for duplicates in cohort (should be one row per subscription_term_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC COUNT(*) AS total_rows, 
# MAGIC COUNT(DISTINCT subscription_term_id) AS unique_terms
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified

# COMMAND ----------

# MAGIC %md ## 7. Label completeness — every qualified term should have a label

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(q.subscription_term_id)   AS cohort_size,
# MAGIC     COUNT(l.subscription_term_id)   AS labeled,
# MAGIC     COUNT(q.subscription_term_id) - COUNT(l.subscription_term_id) AS missing_labels
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC LEFT JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels l
# MAGIC     ON q.subscription_term_id = l.subscription_term_id

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Overview

# COMMAND ----------
# MAGIC %md ## 8. Cohort size over time — subscriptions started per month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', t.term_started_at) AS term_start_month,
# MAGIC     COUNT(DISTINCT q.subscription_id)      AS n_subscriptions
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 9. Label distribution (30-day cancellation rate from term start)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     is_cancelled,
# MAGIC     COUNT(*)                                                    AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels
# MAGIC GROUP BY 1, 2

# COMMAND ----------
# MAGIC %md ## 10. Plan and drug mix

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     pt.drug_strength,
# MAGIC     pt.regimen,
# MAGIC     pt.term_months,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_plan_terms pt
# MAGIC     ON q.subscription_id = pt.subscription_id
# MAGIC WHERE pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2, 3, 4
# MAGIC ORDER BY 5 DESC

# COMMAND ----------
# MAGIC %md ## 11. Geography — subscriptions by user state

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.user_state,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC
