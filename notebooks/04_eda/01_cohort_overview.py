# Databricks notebook source
# 01 — Cohort Overview
# Provides high-level cohort summary stats.
# Run after build_silver.py

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"
QUAL    = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
TERMS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_all_terms"
SUBS    = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
LABELS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_term_start_labels"
EVENTS = f"{CATALOG}.{SCHEMA}.ed_silver_subs_kafka__events"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"


spark.conf.set("eda.qual",    QUAL)
spark.conf.set("eda.terms", TERMS)
spark.conf.set("eda.subs",    SUBS)
spark.conf.set("eda.labels",  LABELS)
spark.conf.set("eda.events",  EVENTS)
spark.conf.set("eda.plan_terms",  PLAN_TERMS)

# COMMAND ----------

# MAGIC %md ## 1. Cohort size

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW subscription_terms_qualified_new AS
# MAGIC SELECT 
# MAGIC *
# MAGIC from ${eda.qual}
# MAGIC where subscription_id not in (
# MAGIC     select
# MAGIC         subscription_id
# MAGIC     from ${eda.terms}
# MAGIC     where term_number = 1
# MAGIC     group by 1
# MAGIC     having count(distinct subscription_term_id) > 1
# MAGIC );
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_terms,
# MAGIC     COUNT(DISTINCT subscription_id) AS unique_subscriptions
# MAGIC FROM subscription_terms_qualified_new

# COMMAND ----------

# MAGIC %md ## 2. Term start date distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', t.term_started_at)::date AS term_start_month,
# MAGIC     COUNT(*) AS n
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC left join ${eda.terms} t
# MAGIC     ON t.subscription_term_id = q.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 desc

# COMMAND ----------

# MAGIC %md ## 3. Cancellation status as of June 30, 2026

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.cancel_status_before_cutoff,
# MAGIC     COUNT(*) AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC left join ${eda.terms} t
# MAGIC on t.subscription_term_id = q.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md ## 4. Cancellation timing — when did cancelled subscribers cancel?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', cancel_requested_at)::date AS cancel_week,
# MAGIC     COUNT(*) AS n
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC left join ${eda.terms} t
# MAGIC     ON t.subscription_term_id = q.subscription_term_id
# MAGIC WHERE is_cancelled_before_cutoff is TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ## 5. Cancellation rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Overall cancellation rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.cancel_status_before_cutoff,
# MAGIC     COUNT(*)                                                    AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC left join ${eda.terms} t
# MAGIC     ON t.subscription_term_id = q.subscription_term_id
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 30-day cancellation rate from term start

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     COUNT(*)                                                    AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC left join general_scratch_catalog.general_scratch.ed_silver_subscription_term_start_labels l
# MAGIC on q.subscription_term_id = l.subscription_term_id
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md ## 6. Latest plan distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Drug name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC LEFT JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC WHERE pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Drug strength

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_strength,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC LEFT JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC WHERE pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Regimen

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.regimen,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC LEFT JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC WHERE pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Cadence

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.term_months cadence,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC WHERE pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md ## 7. Geography — subscriptions by user state

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.user_state,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n_subscriptions,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.subs} s
# MAGIC     ON q.subscription_id = s.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 8. Label window sanity check — day_1_after_start and day_30_after_start
# MAGIC
# MAGIC Verifies that:
# MAGIC - `day_1_after_start` = `term_started_at + 1 day` for all rows
# MAGIC - `day_30_after_start` = `term_started_at + 30 days` for all rows
# MAGIC - No row has `cancel_requested_at` outside the label window incorrectly classified

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should return 0 rows if label window columns are computed correctly
# MAGIC SELECT COUNT(*) AS bad_day_1
# MAGIC FROM ${eda.labels}
# MAGIC WHERE day_1_after_start != DATEADD(DAY, 1, term_started_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS bad_day_30
# MAGIC FROM ${eda.labels}
# MAGIC WHERE day_30_after_start != DATEADD(DAY, 30, term_started_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: cancelled_in_30_days only when cancel_requested_at is within [day_1, day_30]
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS misclassified_cancelled
# MAGIC FROM ${eda.labels}
# MAGIC WHERE cancel_status = 'cancelled_in_30_days'
# MAGIC   AND (cancel_requested_at < day_1_after_start OR cancel_requested_at > day_30_after_start)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: cancelled_at_start only when cancel_requested_at = term_started_at
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS misclassified_at_start
# MAGIC FROM ${eda.labels}
# MAGIC WHERE cancel_status = 'cancelled_at_start'
# MAGIC   AND cancel_requested_at != term_started_at

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot check: sample rows from each cancel_status with their window dates
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     term_started_at,
# MAGIC     day_1_after_start,
# MAGIC     day_30_after_start,
# MAGIC     cancel_requested_at
# MAGIC FROM ${eda.labels}
# MAGIC WHERE cancel_status != 'not_cancelled'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: no day_30_after_start falls after the data pull date (2026-06-30)
# MAGIC -- If any rows exist, the label window extends beyond what we can observe
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS window_exceeds_pull_date
# MAGIC FROM ${eda.labels}
# MAGIC WHERE day_30_after_start > '2026-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: no term_started_at + 1 day falls after the data cutoff (2026-06-01)
# MAGIC -- subscription_terms_qualified filters term_started_at < '2026-06-01',
# MAGIC -- so day_1_after_start should never exceed 2026-06-01
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS day_1_exceeds_cutoff
# MAGIC FROM ${eda.labels}
# MAGIC WHERE day_1_after_start > '2026-06-01'
