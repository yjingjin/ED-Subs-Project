# Databricks notebook source
# MAGIC %md
# MAGIC # 3 First 30-day Cancellation Feature EDA

# COMMAND ----------

# MAGIC %md
# MAGIC This is the **Phase I EDA** for cancellation, where we **set the prediction point (T) at the start of the subscription**. In Phase II, we will explore additional features using rolling prediction points.

# COMMAND ----------

CATALOG    = "general_scratch_catalog"
SCHEMA     = "general_scratch"

LABELS     = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_term_start_labels"
SUBS       = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
TERMS      = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_all_terms"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"
INVOICES   = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
ORDERS     = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_orders"

spark.conf.set("eda.labels",     LABELS)
spark.conf.set("eda.subs",       SUBS)
spark.conf.set("eda.terms",      TERMS)
spark.conf.set("eda.plan_terms", PLAN_TERMS)
spark.conf.set("eda.invoices",   INVOICES)
spark.conf.set("eda.orders",     ORDERS)

from scipy.stats import chi2_contingency, kruskal
import pandas as pd

def chi2_test(df, n_col="n_subscribers", cancelled_col="n_cancelled"):
    """Run chi-square test on a grouped cancellation rate DataFrame."""
    table = [[r[cancelled_col], r[n_col] - r[cancelled_col]] for _, r in df.iterrows()]
    chi2, p, dof, _ = chi2_contingency(table)
    sig = "✓ Significant (p < 0.05)" if p < 0.05 else "✗ Not significant (p ≥ 0.05)"
    print(f"Chi-square: {chi2:.2f} | df: {dof} | p-value: {p:.4f} | {sig}")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 1. Overall cancellation rate (all cancel_status values)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancel_status,
# MAGIC     COUNT(*)                                            AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM ${eda.labels}
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC `cancelled_in_30_days` is the model target. `cancelled_at_start` (day-0 cancellations) are
# MAGIC excluded from sections 2–13 as they are not preventable through a churn prevention model.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 2. Cancellation rate by cadence (billing cycle length)
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.term_months                                           AS cadence,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df2 = spark.sql(f"""
    SELECT pt.term_months AS cadence,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1 ORDER BY 1
""").toPandas()
chi2_test(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** 1-month plan subscribers cancel at a much higher 30-day rate than 3- or 6-month subscribers.
# MAGIC Since subscriptions are auto-renewing, shorter-plan subscribers face a renewal cycle much sooner. To avoid an unwanted charge, they are more likely to cancel proactively within the first 30 days 

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 3. Cancellation rate by drug name
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df3 = spark.sql(f"""
    SELECT pt.drug_name,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** 
# MAGIC Sildenafil users are more likely to cancel within 30 days after the start of subcrition. Sildenafil is as-needed only, so subscribers use it situationally and may feel lower perceived dependency on the medication and less routine reinforcement of the subscription value. In contrast, Tadalafil offers both daily and as-needed dosing options — daily Tadalafil users in particular build a consistent routine, which likely increases retention. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_name,
# MAGIC     pt.regimen,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON l.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 5 DESC

# COMMAND ----------

df3_1 = spark.sql(f"""
    SELECT pt.drug_name,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start' AND regimen = 'AS_NEEDED'
    GROUP BY 1
""").toPandas()
chi2_test(df3_1)

# COMMAND ----------

# MAGIC %md
# MAGIC Further segmented by both drug name and regimen, as-needed users have higher cancellation rates than daily users. **This suggests that the observed drug effect is confounded by regimen.**

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 4. Cancellation rate by regimen
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.regimen,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df4 = spark.sql(f"""
    SELECT pt.regimen,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:**
# MAGIC
# MAGIC As-needed (on-demand) users tend to cancel more than dailu users. Daily dosing implies
# MAGIC a stronger treatment commitment and routine, which may correlate with lower churn.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 5. Cancellation rate by drug strength
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.drug_strength,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df5 = spark.sql(f"""
    SELECT pt.drug_strength,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN pt.drug_strength IN ('2.5mg', '5mg')         THEN 'low (≤5mg)'
# MAGIC         WHEN pt.drug_strength IN ('10mg', '20mg', '25mg') THEN 'mid (10–25mg)'
# MAGIC         WHEN pt.drug_strength IN ('50mg', '100mg')        THEN 'high (≥50mg)'
# MAGIC         ELSE pt.drug_strength
# MAGIC     END AS strength_group,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df5_1 = spark.sql(f"""
    SELECT  
        CASE
            WHEN pt.drug_strength IN ('2.5mg', '5mg')         THEN 'low (≤5mg)'
            WHEN pt.drug_strength IN ('10mg', '20mg', '25mg') THEN 'mid (10–25mg)'
            WHEN pt.drug_strength IN ('50mg', '100mg')        THEN 'high (≥50mg)'
            ELSE pt.drug_strength
        END AS strength_group,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df5_1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** Drug strength is related to cancellation. Low-strength users have lower cancellation rate then mid- and high-strength users.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 6. Cancellation rate by monthly dose
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pt.monthly_dose,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df6 = spark.sql(f"""
    SELECT pt.monthly_dose,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df6)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN pt.monthly_dose <= 8  THEN 'low (≤8/mo)'
# MAGIC         WHEN pt.monthly_dose <= 16 THEN 'mid (9–16/mo)'
# MAGIC         WHEN pt.monthly_dose = 30  THEN 'high (30/mo — daily)'
# MAGIC         ELSE CAST(pt.monthly_dose AS STRING)
# MAGIC     END AS dose_group,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
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

df6_1 = spark.sql(f"""
    SELECT
        CASE
            WHEN pt.monthly_dose <= 8  THEN 'low'
            WHEN pt.monthly_dose <= 16 THEN 'mid'
            WHEN pt.monthly_dose = 30  THEN 'high'
            ELSE CAST(pt.monthly_dose AS STRING)
        END AS dose_group,
        COUNT(DISTINCT l.subscription_id) AS n_subscribers,
        SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt
        ON l.subscription_term_id = pt.subscription_term_id
        AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()

chi2_test(df6_1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** Lower monthly dose is associated with higher cancellation. Daily subscribers (30 doses/mo) cancel at less than half the rate of the lowest-dose group — consistent with the regimen finding that daily dosing builds a routine that reinforces retention.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 7. Cancellation rate by plan change count
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Plan changes within a subscription term **always occur after term_started_at** (our EDA prediction point). This feature will become meaningful in rolling window modeling where the prediction point is mid-term, allowing plan changes that occurred before the scoring date to be counted.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 8. Cancellation rate by payment failure history
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH invoice_stats AS (
# MAGIC     SELECT l.subscription_term_id,
# MAGIC            SUM(CASE WHEN i.is_failed = TRUE THEN 1 ELSE 0 END) AS failed_invoice_count
# MAGIC     FROM ${eda.invoices} i
# MAGIC     JOIN ${eda.labels} l
# MAGIC     ON l.subscription_term_id = i.subscription_term_id
# MAGIC     WHERE i.created_at <= l.term_started_at
# MAGIC         AND (i.failed_at is null OR i.failed_at <= l.term_started_at)
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     ist.failed_invoice_count,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN invoice_stats ist ON l.subscription_term_id = ist.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC Very few subscriptions show payment failure at activation, which makes sense because activation requires a successful payment.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 9. Cancellation rate by acquisition channel
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.first_channel_grouping,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

df11 = spark.sql(f"""
    SELECT s.first_channel_grouping,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {SUBS} s ON l.subscription_id = s.subscription_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df11)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN s.first_channel_grouping IN ('organic search', 'paid search')
# MAGIC             THEN 'active_search'
# MAGIC         WHEN s.first_channel_grouping IN ('direct', 'crm')
# MAGIC             THEN 're_engaged'
# MAGIC         ELSE 'unknown_other'
# MAGIC     END AS channel_group,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 4 DESC

# COMMAND ----------

from scipy.stats import chi2_contingency

df = spark.sql(f"""
    SELECT
        CASE
            WHEN s.first_channel_grouping IN ('organic search', 'paid search')
                THEN 'active_search'
            WHEN s.first_channel_grouping IN ('direct', 'crm')
                THEN 're_engaged'
            ELSE 'unknown_other'
        END AS channel_group,
        COUNT(DISTINCT l.subscription_id) AS n_subscribers,
        SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {SUBS} s ON l.subscription_id = s.subscription_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()

table = [[r["n_cancelled"], r["n_subscribers"] - r["n_cancelled"]] for _, r in df.iterrows()]
chi2, p, dof, _ = chi2_contingency(table)
sig = "✓ Significant (p < 0.05)" if p < 0.05 else "✗ Not significant (p ≥ 0.05)"
print(f"Chi-square: {chi2:.2f} | df: {dof} | p-value: {p:.4f} | {sig}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** Users acquired through active search channels, including paid and organic search, have higher cancellation rates than re-engaged users from direct and CRM channels. Users acquired through unknown or other sources have the highest cancellation rates.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 12. Cancellation rate by platform
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.first_platform,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

df12 = spark.sql(f"""
    SELECT s.first_platform,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {SUBS} s ON l.subscription_id = s.subscription_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df12)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** acquisition chanel has no impact on cancellation.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 13. Cancellation rate by user state (states with ≥ 100 subscribers)
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.user_state,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.subs} s ON l.subscription_id = s.subscription_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC HAVING COUNT(DISTINCT l.subscription_id) >= 100
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

df13 = spark.sql(f"""
    SELECT s.user_state,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {SUBS} s ON l.subscription_id = s.subscription_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
    HAVING COUNT(DISTINCT l.subscription_id) >= 100
""").toPandas()
chi2_test(df13)

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** Cancellation rates vary across user states.
