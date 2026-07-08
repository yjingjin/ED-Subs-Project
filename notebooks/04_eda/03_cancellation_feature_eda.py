# Databricks notebook source
# 03 — Cancellation Feature EDA (with statistical tests)
#
# Cohort: activated, non-reactivated subscribers (ed_silver_subscription_term_start_labels)
# Label: cancel_status — cancelled_in_30_days vs not_cancelled
#
# NOTE: Sections 2–14 exclude day-0 cancellations (cancel_status = 'cancelled_at_start').
# Reason: same-day cancellations occur before the model could act and do not represent
# preventable churn. Including them would inflate cancel rates and bias feature analysis
# toward features correlated with low activation quality rather than true churn intent.
# Section 1 is the only section that reports all three cancel_status values.
#
# Statistical test used: Chi-square test of independence (scipy.stats.chi2_contingency).
# Null hypothesis: cancellation rate is the same across all segments.
# p < 0.05 → the feature is statistically significantly associated with cancellation.

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
# MAGIC **Findings:** []
# MAGIC
# MAGIC `cancelled_in_30_days` is the model target. `cancelled_at_start` (day-0 cancellations) are
# MAGIC excluded from sections 2–14 as they are not preventable through a churn prevention model.

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 2. Cancellation rate by cadence (term length)
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
# MAGIC **Findings:** []
# MAGIC
# MAGIC 1-month plan subscribers cancel at a much higher rate than 3- or 6-month subscribers.
# MAGIC The shorter the commitment, the lower the friction to cancel.
# MAGIC **Cadence is expected to be one of the strongest features in the model.**

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
# MAGIC **Findings:** []

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
# MAGIC **Findings:** []
# MAGIC
# MAGIC AS_NEEDED (on-demand) users tend to cancel more than DAILY users. Daily dosing implies
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
# MAGIC %md
# MAGIC **Findings:** []

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
# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 7. Cancellation rate by plan change count
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH plan_change_counts AS (
# MAGIC     SELECT subscription_term_id,
# MAGIC            COUNT(*) - 1 AS plan_change_count
# MAGIC     FROM ${eda.plan_terms}
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     pc.plan_change_count,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN plan_change_counts pc ON l.subscription_term_id = pc.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

df7 = spark.sql(f"""
    WITH plan_change_counts AS (
        SELECT subscription_term_id, COUNT(*) - 1 AS plan_change_count
        FROM {PLAN_TERMS} GROUP BY 1
    )
    SELECT pc.plan_change_count,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN plan_change_counts pc ON l.subscription_term_id = pc.subscription_term_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df7)

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** []
# MAGIC
# MAGIC Subscribers who change plans are more engaged — they are actively optimizing their treatment
# MAGIC rather than passively continuing. This tends to correlate with lower cancel rates.

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 8. Cancellation rate by payment failure history
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH invoice_stats AS (
# MAGIC     SELECT subscription_term_id,
# MAGIC            SUM(CASE WHEN is_failed = TRUE THEN 1 ELSE 0 END) AS failed_invoice_count
# MAGIC     FROM ${eda.invoices}
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

df8 = spark.sql(f"""
    WITH invoice_stats AS (
        SELECT subscription_term_id,
               SUM(CASE WHEN is_failed = TRUE THEN 1 ELSE 0 END) AS failed_invoice_count
        FROM {INVOICES} GROUP BY 1
    )
    SELECT ist.failed_invoice_count,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN invoice_stats ist ON l.subscription_term_id = ist.subscription_term_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df8)

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 9. Cancellation rate by delinquency
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_delinquent,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.terms} t ON l.subscription_term_id = t.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

df9 = spark.sql(f"""
    SELECT t.is_delinquent,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN {TERMS} t ON l.subscription_term_id = t.subscription_term_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df9)

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 10. Cancellation rate by order count (fulfillment behavior)
# MAGIC *(excludes day-0 cancellations)*

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_counts AS (
# MAGIC     SELECT inv.subscription_term_id,
# MAGIC            COUNT(DISTINCT inv.invoice_id) AS order_count
# MAGIC     FROM ${eda.invoices} inv
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     oc.order_count,
# MAGIC     COUNT(DISTINCT l.subscription_id)                        AS n_subscribers,
# MAGIC     SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1.0 ELSE 0 END)
# MAGIC         / COUNT(DISTINCT l.subscription_id) * 100, 1)        AS cancel_rate_pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN order_counts oc ON l.subscription_term_id = oc.subscription_term_id
# MAGIC WHERE l.cancel_status != 'cancelled_at_start'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

df10 = spark.sql(f"""
    WITH order_counts AS (
        SELECT subscription_term_id, COUNT(DISTINCT invoice_id) AS order_count
        FROM {INVOICES} GROUP BY 1
    )
    SELECT oc.order_count,
           COUNT(DISTINCT l.subscription_id) AS n_subscribers,
           SUM(CASE WHEN l.cancel_status = 'cancelled_in_30_days' THEN 1 ELSE 0 END) AS n_cancelled
    FROM {LABELS} l
    JOIN order_counts oc ON l.subscription_term_id = oc.subscription_term_id
    WHERE l.cancel_status != 'cancelled_at_start'
    GROUP BY 1
""").toPandas()
chi2_test(df10)

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 11. Cancellation rate by acquisition channel
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
# MAGIC %md
# MAGIC **Findings:** []

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
# MAGIC **Findings:** []

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
# MAGIC **Findings:** []

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 14. Days to cancel distribution by cadence
# MAGIC *(excludes day-0 cancellations — cancelled_in_30_days only)*
# MAGIC
# MAGIC Statistical test: Kruskal-Wallis test (non-parametric comparison of distributions across cadence groups).
# MAGIC Null hypothesis: the distribution of days-to-cancel is the same across all cadence groups.

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

# COMMAND ----------

df14 = spark.sql(f"""
    SELECT pt.term_months AS cadence,
           DATEDIFF(DAY, l.prediction_point, l.cancel_requested_at) AS days_to_cancel
    FROM {LABELS} l
    JOIN {PLAN_TERMS} pt ON l.subscription_term_id = pt.subscription_term_id AND pt.is_latest_plan_term = TRUE
    WHERE l.cancel_status = 'cancelled_in_30_days'
""").toPandas()

groups = [grp["days_to_cancel"].dropna().tolist() for _, grp in df14.groupby("cadence")]
if len(groups) >= 2:
    stat, p = kruskal(*groups)
    sig = "✓ Significant (p < 0.05)" if p < 0.05 else "✗ Not significant (p ≥ 0.05)"
    print(f"Kruskal-Wallis H: {stat:.2f} | p-value: {p:.4f} | {sig}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Findings:** []
# MAGIC
# MAGIC Cancellation timing differs by cadence — 1-month plan subscribers tend to cancel around the
# MAGIC refill reminder window (~day 23), while 3-month and 6-month subscribers cancel later in the term.
# MAGIC This suggests that reminders and engagement touchpoints may need to be cadence-specific.
