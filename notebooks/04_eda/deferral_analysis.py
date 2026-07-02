# Databricks notebook source
# Pre-EDA: Deferral Analysis
# Explores term_renewal_time_changed and upcoming_term_renewal_notified events to understand:
#   1. Volume and proportion of deferrals in the cohort
#   2. Whether deferred renewals actually renew (vs churn)
#   3. Proportion of cancellations among deferred vs non-deferred subscribers

# COMMAND ----------

CATALOG = "general_scratch_catalog"
SCHEMA  = "general_scratch"

EVENTS   = f"{CATALOG}.{SCHEMA}.ed_bronze_int_subs_kafka__events"
TERMS    = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms"
LABELS   = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_labels"
QUAL     = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions_qualified"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Check

# COMMAND ----------

# MAGIC %md
# MAGIC Any term renewal time changes without old and new renewal days?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC subscription_id,
# MAGIC event_name,
# MAGIC changed_by,
# MAGIC old_renewal_at,
# MAGIC new_renewal_at 
# MAGIC from general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC where event_name = 'term_renewal_time_changed'
# MAGIC     and changed_by = 'CHANGED_BY_USER'
# MAGIC     and old_renewal_at is null 
# MAGIC     and new_renewal_at is null

# COMMAND ----------

# MAGIC %md ## 1. Overall event volume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     event_name,
# MAGIC     changed_by,
# MAGIC     COUNT(*)                                          AS n_events,
# MAGIC     COUNT(DISTINCT subscription_id)                  AS n_subscriptions
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC GROUP BY event_name, changed_by
# MAGIC ORDER BY n_events DESC

# COMMAND ----------

# MAGIC %md ## 2. Proportion of subscribers who deferred at least once

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cohort AS (
# MAGIC     SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC ),
# MAGIC deferred AS (
# MAGIC     SELECT DISTINCT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC     WHERE event_name = 'term_renewal_time_changed'
# MAGIC     AND changed_by = 'CHANGED_BY_USER'
# MAGIC )
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT c.subscription_id)                          AS cohort_size,
# MAGIC     COUNT(DISTINCT d.subscription_id)                         AS n_deferred,
# MAGIC     round(COUNT(DISTINCT d.subscription_id)
# MAGIC         / COUNT(DISTINCT c.subscription_id) * 100,2)             AS pct_deferred,
# MAGIC     COUNT(DISTINCT c.subscription_id)
# MAGIC         - COUNT(DISTINCT d.subscription_id)                   AS n_never_deferred
# MAGIC FROM cohort c
# MAGIC LEFT JOIN deferred d ON c.subscription_id = d.subscription_id

# COMMAND ----------

# MAGIC %md ## 3. Deferral frequency — how many times did subscribers defer?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cohort AS (
# MAGIC     SELECT subscription_id 
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC ),
# MAGIC deferral_counts AS (
# MAGIC     SELECT
# MAGIC         e.subscription_id,
# MAGIC         COUNT(*) AS n_deferrals
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events e
# MAGIC     JOIN cohort c ON e.subscription_id = c.subscription_id
# MAGIC     WHERE e.event_name = 'term_renewal_time_changed'
# MAGIC         AND changed_by = 'CHANGED_BY_USER'
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     n_deferrals,
# MAGIC     COUNT(*) AS n_subscriptions
# MAGIC FROM deferral_counts
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ## 4. How far did subscribers push their renewal? (days deferred)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cohort AS (
# MAGIC     SELECT 
# MAGIC     subscription_id 
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, old_renewal_at::date, new_renewal_at::date) AS days_deferred,
# MAGIC     COUNT(*)                                                   AS n_events
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events e
# MAGIC JOIN cohort c ON e.subscription_id = c.subscription_id
# MAGIC WHERE e.event_name = 'term_renewal_time_changed'
# MAGIC   AND changed_by = 'CHANGED_BY_USER' 
# MAGIC   AND old_renewal_at IS NOT NULL
# MAGIC   AND new_renewal_at IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 desc

# COMMAND ----------

# MAGIC %md ## 5. Did deferred subscribers actually renew?

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %sql
# MAGIC WITH deferred AS (
# MAGIC     SELECT
# MAGIC         subscription_id,
# MAGIC         MAX(new_renewal_at) AS new_renewal_at   -- latest deferral per sub
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC     WHERE event_name = 'term_renewal_time_changed'
# MAGIC       AND changed_by = 'CHANGED_BY_USER'
# MAGIC     GROUP BY 1
# MAGIC     having new_renewal_at <= DATEADD(DAY, -30, '2026-06-30')  -- ensure 30-day obs window
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     CASE WHEN i.subscription_id IS NOT NULL THEN 'yes' ELSE 'no' END AS is_renewed,
# MAGIC     COUNT(*) AS n_subscriptions,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_subscriptions
# MAGIC FROM deferred d
# MAGIC LEFT JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_invoices i
# MAGIC     ON i.subscription_id = d.subscription_id
# MAGIC     AND i.created_at::date >= d.new_renewal_at::date
# MAGIC --    AND abs(datediff(day, i.created_at::date,d.new_renewal_at::date)) <= 7
# MAGIC    AND i.is_paid = true
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deferred AS (
# MAGIC     SELECT
# MAGIC         subscription_id,
# MAGIC         MAX(new_renewal_at) AS new_renewal_at   -- latest deferral per sub
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC     WHERE event_name = 'term_renewal_time_changed'
# MAGIC       AND changed_by = 'CHANGED_BY_USER'
# MAGIC     GROUP BY 1
# MAGIC     having new_renewal_at <= DATEADD(DAY, -30, '2026-06-30')  -- ensure 30-day obs window
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC subscription_id,
# MAGIC new_renewal_at
# MAGIC from (SELECT
# MAGIC     d.subscription_id,
# MAGIC     CASE WHEN i.subscription_id IS NOT NULL THEN 'yes' ELSE 'no' END AS is_renewed,
# MAGIC     new_renewal_at
# MAGIC FROM deferred d
# MAGIC LEFT JOIN general_scratch_catalog.general_scratch.ed_silver_subscription_invoices i
# MAGIC     ON i.subscription_id = d.subscription_id
# MAGIC     AND i.created_at::date >= d.new_renewal_at::date
# MAGIC --    AND abs(datediff(day, i.created_at::date,d.new_renewal_at::date)) <= 7
# MAGIC    AND i.is_paid = true) temp
# MAGIC    where is_renewed = 'no'
# MAGIC    limit 10
# MAGIC

# COMMAND ----------

# MAGIC %md ## 6. Cancellation rate: deferred vs never deferred

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deferred AS (
# MAGIC     SELECT DISTINCT subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events
# MAGIC     WHERE event_name = 'term_renewal_time_changed'
# MAGIC ),
# MAGIC cohort AS (
# MAGIC     SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE WHEN d.subscription_id IS NOT NULL THEN 'deferred' ELSE 'never deferred' END AS deferral_group,
# MAGIC     t.term_status,
# MAGIC     t.termination_type,
# MAGIC     COUNT(DISTINCT t.subscription_id)                       AS n_subscriptions
# MAGIC FROM general_scratch_catalog.general_scratch.ed_silver_subscription_terms t
# MAGIC JOIN cohort c ON t.subscription_id = c.subscription_id
# MAGIC LEFT JOIN deferred d ON t.subscription_id = d.subscription_id
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, 4 DESC

# COMMAND ----------

# MAGIC %md ## 7. Timing: when do deferrals happen relative to the renewal date?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Days before original renewal date that the deferral event occurred
# MAGIC WITH cohort AS (
# MAGIC     SELECT subscription_id FROM general_scratch_catalog.general_scratch.ed_silver_subscriptions_qualified
# MAGIC )
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, e.occurred_at::date, e.old_renewal_at::date) AS days_before_renewal,
# MAGIC     COUNT(*)                                                    AS n_events
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_int_subs_kafka__events e
# MAGIC JOIN cohort c ON e.subscription_id = c.subscription_id
# MAGIC WHERE e.event_name = 'term_renewal_time_changed'
# MAGIC   AND e.old_renewal_at IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1
