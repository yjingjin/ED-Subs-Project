# Databricks notebook source
# Sanity checks for ed_silver_subscription_terms_qualified and ed_silver_subscription_labels.
# Run after build_silver.py to validate the cohort and label before EDA/modeling.
#
# ed_silver_subscription_terms_qualified contains only: subscription_id, subscription_term_id
# Additional term attributes are joined from ed_bronze_subscription_terms as needed.

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"
QUAL    = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
LABELS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_labels"
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
# MAGIC %md ## 2. Term status distribution — all should be active at snapshot

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.term_status,
# MAGIC     COUNT(*) AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 3. Verify no prior cancellation requests in cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS n_with_prior_cancel
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC WHERE t.cancel_requested_at <= '2026-05-01'

# COMMAND ----------
# MAGIC %md ## 4. Verify no already-ended terms in cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should return 0 rows
# MAGIC SELECT COUNT(*) AS n_ended_before_snapshot
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC WHERE t.term_ended_at <= '2026-05-01'

# COMMAND ----------
# MAGIC %md ## 5. Label distribution — cancellation rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     is_cancelled,
# MAGIC     COUNT(*)                                                      AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)           AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_labels
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 2 DESC

# COMMAND ----------
# MAGIC %md ## 6. Cross-tab: cancel_status vs current subscription status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancelled + active should be near zero (main sanity check)
# MAGIC SELECT
# MAGIC     l.cancel_status,
# MAGIC     s.status,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_labels l
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_silver_subscriptions s
# MAGIC     ON l.subscription_id = s.subscription_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------
# MAGIC %md ## 7. Term start date distribution — confirm all started on or before 2026-05-01

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', t.term_started_at) AS term_start_month,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 8. Cancellation timing — when did cancelled subscribers cancel?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('week', cancel_requested_at) AS cancel_week,
# MAGIC     COUNT(*) AS n
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_labels
# MAGIC WHERE is_cancelled = 1
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------
# MAGIC %md ## 9. Check for duplicates in cohort (should be one row per subscription_term_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT subscription_term_id) AS unique_terms
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified

# COMMAND ----------
# MAGIC %md ## 10. Label completeness — every qualified term should have a label

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(q.subscription_term_id)   AS cohort_size,
# MAGIC     COUNT(l.subscription_term_id)   AS labeled,
# MAGIC     COUNT(q.subscription_term_id) - COUNT(l.subscription_term_id) AS missing_labels
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms_qualified q
# MAGIC LEFT JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_labels l
# MAGIC     ON q.subscription_term_id = l.subscription_term_id
