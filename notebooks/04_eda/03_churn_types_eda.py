# Databricks notebook source
# MAGIC %md
# MAGIC #3. Cancellation Type EDA: Voluntary vs Involuntary Churn
# MAGIC
# MAGIC Goal: understand the split between voluntary churn (cancel request) and
# MAGIC involuntary churn (payment failure), compare their characteristics,
# MAGIC and justify excluding involuntary churn from the prevention model.

# COMMAND ----------

CATALOG    = "general_scratch_catalog"
SCHEMA     = "general_scratch"

QUAL       = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
TERMS      = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_all_terms"
TERMS_B    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"
SUBS       = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
INVOICES   = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
CHARGES    = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_charges"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"
LABELS     = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_30d_cancel_label"
EVENTS     = f"{CATALOG}.{SCHEMA}.ed_silver_subs_kafka__events"

spark.conf.set("eda.qual",       QUAL)
spark.conf.set("eda.terms",      TERMS)
spark.conf.set("eda.terms_b",    TERMS_B)
spark.conf.set("eda.subs",       SUBS)
spark.conf.set("eda.invoices",   INVOICES)
spark.conf.set("eda.charges",    CHARGES)
spark.conf.set("eda.plan_terms", PLAN_TERMS)
spark.conf.set("eda.labels",     LABELS)
spark.conf.set("eda.events",     EVENTS)

from scipy.stats import chi2_contingency
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

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

# MAGIC %md ---
# MAGIC ## 1. Overall Churn Classification
# MAGIC
# MAGIC Classify each subscription term as follows:
# MAGIC
# MAGIC - **Churned**: A subscription term is considered effectively **churned** if both `term_started_at` and `term_ended_at` are not null.
# MAGIC     - **Voluntary churn**: the subscriber submitted a cancellation request (`cancel_requested_at IS NOT NULL`)
# MAGIC     
# MAGIC     - **Involuntary churn**: the term ended without a cancellation request, such as due to payment failure (`cancel_requested_at IS NULL`)
# MAGIC
# MAGIC - **Active / retained**: `term_started_at` of a subscription is not null but `term_ended_at` is null.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN (t.term_ended_at IS NOT NULL AND t.term_ended_at::date <= DATE '2026-06-30') AND t.cancel_requested_at IS NOT NULL                          THEN 'voluntary_churn'
# MAGIC         WHEN (t.term_ended_at IS NOT NULL AND t.term_ended_at::date <= DATE '2026-06-30')  AND t.cancel_requested_at IS NULL                              THEN 'involuntary_churn'
# MAGIC         WHEN (t.term_ended_at IS NULL OR t.term_ended_at::date > DATE '2026-06-30') THEN 'retained'
# MAGIC         ELSE NULL
# MAGIC     END AS churn_type,
# MAGIC     COUNT(DISTINCT q.subscription_id)                                   AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1)          AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     distinct is_failed_payment_canceled
# MAGIC from general_scratch_catalog.general_scratch.ed_bronze_subscription_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     t.termination_type,
# MAGIC     COUNT(DISTINCT t.subscription_id)
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t 
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC -- LEFT JOIN ${eda.events} AS cancels
# MAGIC --     ON q.subscription_id = cancels.subscription_id
# MAGIC --     AND cancels.event_name = 'canceled'
# MAGIC WHERE
# MAGIC     t.term_started_at IS NOT NULL
# MAGIC     AND t.term_ended_at IS NOT NULL
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1 DESC,3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC `is_failed_payment_canceled` only takes the values `false` and `null`. However, when it is `null`, the subscription terms ended due to payment failure.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     t.cancel_requested_at IS NULL AS did_not_submit_cancel_request,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (partition by t.is_failed_payment_canceled), 2) AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t 
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC -- LEFT JOIN ${eda.events} AS cancels
# MAGIC --     ON q.subscription_id = cancels.subscription_id
# MAGIC --     AND cancels.event_name = 'canceled'
# MAGIC WHERE
# MAGIC     t.term_started_at IS NOT NULL
# MAGIC     AND t.term_ended_at IS NOT NULL
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1 DESC,3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Most rows with `is_failed_payment_canceled = false` have a cancellation request, whereas most rows with `is_failed_payment_canceled = true` do not.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     attempt_number,
# MAGIC     attempts_remaining,
# MAGIC     COUNT(DISTINCT subscription_id)
# MAGIC FROM ${eda.charges}
# MAGIC WHERE
# MAGIC     was_paid IS FALSE
# MAGIC     AND subscription_category = 'conditions'
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at IS NOT NULL                          THEN 'voluntary_churn'
# MAGIC         WHEN t.is_failed_payment_canceled = TRUE
# MAGIC          AND t.cancel_requested_at IS NULL                              THEN 'involuntary_churn'
# MAGIC         ELSE 'active_or_retained'
# MAGIC     END AS churn_type,
# MAGIC     COUNT(DISTINCT q.subscription_id)                                   AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (), 1)          AS pct
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t ON q.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 2. Voluntary vs involuntary — plan type comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at IS NOT NULL THEN 'voluntary'
# MAGIC         WHEN t.is_failed_payment_canceled = TRUE THEN 'involuntary'
# MAGIC         ELSE 'retained'
# MAGIC     END AS churn_type,
# MAGIC     pt.term_months                                              AS cadence,
# MAGIC     pt.drug_name,
# MAGIC     pt.regimen,
# MAGIC     CASE
# MAGIC         WHEN pt.drug_strength IN ('2.5mg','5mg')         THEN 'low (≤5mg)'
# MAGIC         WHEN pt.drug_strength IN ('10mg','20mg','25mg')  THEN 'mid (10–25mg)'
# MAGIC         WHEN pt.drug_strength IN ('50mg','100mg')        THEN 'high (≥50mg)'
# MAGIC     END AS strength_group,
# MAGIC     CASE
# MAGIC         WHEN pt.monthly_dose <= 8  THEN 'low (≤8/mo)'
# MAGIC         WHEN pt.monthly_dose <= 16 THEN 'mid (9–16/mo)'
# MAGIC         WHEN pt.monthly_dose = 30  THEN 'high (30/mo)'
# MAGIC     END AS dose_group,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY CASE
# MAGIC                   WHEN t.cancel_requested_at IS NOT NULL THEN 'voluntary'
# MAGIC                   WHEN t.is_failed_payment_canceled = TRUE THEN 'involuntary'
# MAGIC                   ELSE 'retained' END
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t ON q.subscription_term_id = t.subscription_term_id
# MAGIC JOIN ${eda.plan_terms} pt ON q.subscription_term_id = pt.subscription_term_id
# MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1,2,3,4,5,6
# MAGIC ORDER BY 1,7 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 3. Voluntary vs involuntary — payment behavior

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Avg number of charge attempts, failure count, and delinquency by churn type
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at IS NOT NULL THEN 'voluntary'
# MAGIC         WHEN t.is_failed_payment_canceled = TRUE THEN 'involuntary'
# MAGIC         ELSE 'retained'
# MAGIC     END AS churn_type,
# MAGIC     ROUND(AVG(inv.num_charges), 2)                              AS avg_num_charges,
# MAGIC     ROUND(AVG(CASE WHEN inv.is_failed THEN 1.0 ELSE 0 END), 3) AS avg_failure_rate,
# MAGIC     ROUND(AVG(CASE WHEN inv.is_delinquent THEN 1.0 ELSE 0 END), 3) AS avg_delinquency_rate,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t ON q.subscription_term_id = t.subscription_term_id
# MAGIC JOIN ${eda.invoices} inv ON q.subscription_term_id = inv.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 4. Voluntary vs involuntary — timing (days from term start to churn)

# COMMAND ----------

df4 = spark.sql(f"""
    SELECT
        CASE
            WHEN t.cancel_requested_at IS NOT NULL THEN 'voluntary'
            WHEN t.is_failed_payment_canceled = TRUE THEN 'involuntary'
        END AS churn_type,
        DATEDIFF(DAY, t.term_started_at::date,
                 COALESCE(t.cancel_requested_at::date, t.term_ended_at::date)) AS days_to_churn
    FROM subscription_terms_qualified_new q
    JOIN {TERMS_B} t ON q.subscription_term_id = t.subscription_term_id
    WHERE t.cancel_requested_at IS NOT NULL OR t.is_failed_payment_canceled = TRUE
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 5))
for churn_type, color in [("voluntary", "#E53935"), ("involuntary", "#FF9800")]:
    vals = df4[df4["churn_type"] == churn_type]["days_to_churn"].dropna()
    ax.hist(vals, bins=60, alpha=0.5, color=color, label=f"{churn_type} (n={len(vals):,})", density=True)
ax.set_xlabel("Days from term start to churn event")
ax.set_ylabel("Density")
ax.set_title("Churn Timing: Voluntary vs Involuntary", fontweight="bold")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 5. Cancellation label coverage — how much involuntary churn does the label capture?
# MAGIC
# MAGIC The model label (`cancel_status = 'cancelled_in_30_days'`) is based on `cancel_requested_at`.
# MAGIC Involuntary churners (payment failure) typically do NOT submit a cancel request —
# MAGIC they are terminated by the system. This section measures the overlap.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     l.cancel_status,
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     COUNT(DISTINCT l.subscription_id) AS n,
# MAGIC     ROUND(COUNT(DISTINCT l.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT l.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN ${eda.terms_b} t ON l.subscription_term_id = t.subscription_term_id
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:**
# MAGIC - `cancelled_in_30_days + is_failed_payment_canceled = FALSE` → voluntary churn correctly captured by label
# MAGIC - `cancelled_in_30_days + is_failed_payment_canceled = TRUE` → rare: subscriber had both a cancel request and a payment failure
# MAGIC - `not_cancelled + is_failed_payment_canceled = TRUE` → involuntary churn NOT captured by label (as expected)
# MAGIC
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 6. Why exclude involuntary churn from the prevention model?
# MAGIC
# MAGIC | | Voluntary churn | Involuntary churn |
# MAGIC | --- | --- | --- |
# MAGIC | Trigger | User decision | Payment system failure |
# MAGIC | Signal | Behavioral disengagement, dissatisfaction | Payment method issue, billing error |
# MAGIC | Intervention | Retention offer, engagement campaign | Payment recovery, card update nudge |
# MAGIC | Model target | ✓ Cancel request within 30 days | ✗ Not captured by cancel_requested_at |
# MAGIC | Operational owner | Retention/growth team | Payments/ops team |
# MAGIC
# MAGIC **Conclusion:** Involuntary churners are already excluded from the model label (they don't submit
# MAGIC cancel requests, so they appear as label=0 in the training data). The model is specifically a
# MAGIC *cancellation prevention* model — it predicts user-initiated cancellations where intervention
# MAGIC can change the outcome. Involuntary churn requires a different solution (payment recovery) and
# MAGIC should be handled separately.
