# Databricks notebook source
# MAGIC %md
# MAGIC #3. Cancellation Types EDA: Voluntary vs Involuntary
# MAGIC
# MAGIC Goal: understand the split between voluntary cancellation (cancel request) and
# MAGIC involuntary cancellation (term ended without a cancel request, e.g. delinquency),
# MAGIC and compare their characteristics.
# MAGIC
# MAGIC Snapshot / label cutoff: **2026-06-30**.

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
# MAGIC     *
# MAGIC FROM ${eda.qual}
# MAGIC WHERE subscription_id NOT IN (
# MAGIC     SELECT
# MAGIC         subscription_id
# MAGIC     FROM ${eda.terms}
# MAGIC     WHERE term_number = 1
# MAGIC     GROUP BY 1
# MAGIC     HAVING COUNT(DISTINCT subscription_term_id) > 1
# MAGIC );
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_terms,
# MAGIC     COUNT(DISTINCT subscription_id) AS unique_subscriptions
# MAGIC FROM subscription_terms_qualified_new

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 1. Cancellation type definition
# MAGIC
# MAGIC Cutoff date: **2026-06-30**
# MAGIC
# MAGIC Classify each qualified subscription term as:
# MAGIC
# MAGIC - **`voluntary_cancellation`**: subscriber requested cancel on or before the cutoff
# MAGIC   (`cancel_requested_at::date <= 2026-06-30`)
# MAGIC - **`involuntary_cancellation`**: term ended on or before the cutoff with **no** cancel request
# MAGIC   (`term_ended_at <= 2026-06-30` AND `cancel_requested_at IS NULL`)
# MAGIC - **`not_cancelled`**: still active as of the cutoff, or cancel/term end only after the cutoff
# MAGIC   (`term_ended_at` null or after cutoff, **or** `cancel_requested_at` after cutoff)
# MAGIC
# MAGIC Voluntary takes priority over involuntary when both could apply.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW subscription_terms_qualified_new_labeled AS
# MAGIC SELECT
# MAGIC     q.*,
# MAGIC     CASE
# MAGIC         WHEN t.cancel_requested_at::date <= DATE '2026-06-30'
# MAGIC             THEN 'voluntary_cancellation'
# MAGIC         WHEN t.term_ended_at IS NOT NULL
# MAGIC          AND t.term_ended_at::date <= DATE '2026-06-30'
# MAGIC          AND t.cancel_requested_at IS NULL
# MAGIC             THEN 'involuntary_cancellation'
# MAGIC         WHEN t.term_ended_at IS NULL
# MAGIC           OR t.term_ended_at::date > DATE '2026-06-30'
# MAGIC           OR t.cancel_requested_at::date > DATE '2026-06-30'
# MAGIC             THEN 'not_cancelled'
# MAGIC         ELSE NULL
# MAGIC     END AS cancellation_type
# MAGIC FROM subscription_terms_qualified_new q
# MAGIC JOIN ${eda.terms_b} t ON q.subscription_term_id = t.subscription_term_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cancellation_type,
# MAGIC     COUNT(DISTINCT subscription_id)                                   AS n,
# MAGIC     ROUND(COUNT(DISTINCT subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT subscription_id)) OVER (), 1)          AS pct
# MAGIC FROM subscription_terms_qualified_new_labeled
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Cancellation due to deliquency

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the classification logic: are `involuntary_cancellation` terms ended due to delinquency?
# MAGIC
# MAGIC Using `is_failed_payment_canceled` from the terms table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT is_failed_payment_canceled
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscription_terms

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     attempt_number,
# MAGIC     attempts_remaining,
# MAGIC     COUNT(DISTINCT c.subscription_id) AS n_subs
# MAGIC FROM ${eda.charges} AS c
# MAGIC JOIN subscription_terms_qualified_new_labeled q
# MAGIC     ON c.subscription_id = q.subscription_id
# MAGIC WHERE c.was_paid = FALSE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1%sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     t.termination_type,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n_subs
# MAGIC FROM subscription_terms_qualified_new_labeled q
# MAGIC JOIN ${eda.terms_b} t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC WHERE t.is_failed_payment_canceled IS NULL
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1 DESC, 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC `is_failed_payment_canceled` only takes the values `false` and `null`. However, when it is `null`, the subscription terms often ended due to delinquency. So when `is_failed_payment_canceled` = `null`, the subs is failed payment canceled.

# COMMAND ----------

# MAGIC %md
# MAGIC Get number of charge attempts from charge table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     attempt_number,
# MAGIC     attempts_remaining,
# MAGIC     COUNT(DISTINCT c.subscription_id) AS n_subs
# MAGIC FROM ${eda.charges} AS c
# MAGIC JOIN subscription_terms_qualified_new_labeled q
# MAGIC     ON c.subscription_id = q.subscription_id
# MAGIC WHERE c.was_paid = FALSE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC Max charge number is 8.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     t.termination_type,
# MAGIC     cancels.changed_by,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n_subs
# MAGIC FROM subscription_terms_qualified_new_labeled q
# MAGIC JOIN ${eda.terms_b} t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC LEFT JOIN ${eda.events} AS cancels
# MAGIC     ON q.subscription_id = cancels.subscription_id
# MAGIC     AND cancels.event_name = 'canceled'
# MAGIC WHERE q.subscription_id IN (
# MAGIC     SELECT subscription_id
# MAGIC     FROM ${eda.charges}
# MAGIC     WHERE was_paid = FALSE
# MAGIC       AND attempt_number = 8
# MAGIC )
# MAGIC and cancellation_type = 'involuntary_cancellation'
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 4 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC  There are subscriptions whose charge attempts all fail (e.g. `attempt_number = 8` and `was_paid` is false). Nearly all map to `is_failed_payment_canceled is null` and are canceled by the system.

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** when `is_failed_payment_canceled is null`, the subscription is cancelled due to payment failure. `is_failed_payment_canceled is null` -> involuntary cacellation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Does involuntary cancellation all caused by payment failure?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n_subs,
# MAGIC     ROUND(COUNT(DISTINCT t.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT t.subscription_id)) OVER (), 1)          AS pct
# MAGIC FROM subscription_terms_qualified_new_labeled q
# MAGIC JOIN ${eda.terms_b} t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC WHERE q.cancellation_type = 'involuntary_cancellation'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.is_failed_payment_canceled,
# MAGIC     t.termination_type,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n_subs,
# MAGIC     ROUND(COUNT(DISTINCT t.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT t.subscription_id)) OVER (), 1)          AS pct
# MAGIC FROM subscription_terms_qualified_new_labeled q
# MAGIC JOIN ${eda.terms_b} t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC WHERE q.cancellation_type = 'involuntary_cancellation'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Most involuntary cancellations are failed payment canceled, while some are not. However, for those subscriptions, `termination_type` is mostly `UNKNOWN`.

# COMMAND ----------

# MAGIC %md
# MAGIC For those with `is_failed_payment_canceled = false`, find out who made the change.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.termination_type,
# MAGIC     cancels.changed_by,
# MAGIC     COUNT(DISTINCT t.subscription_id) AS n_subs
# MAGIC FROM subscription_terms_qualified_new_labeled q
# MAGIC JOIN ${eda.terms_b} t
# MAGIC     ON q.subscription_term_id = t.subscription_term_id
# MAGIC LEFT JOIN ${eda.events} AS cancels
# MAGIC     ON q.subscription_id = cancels.subscription_id
# MAGIC     AND cancels.event_name = 'canceled'
# MAGIC WHERE q.cancellation_type = 'involuntary_cancellation'
# MAGIC   AND t.is_failed_payment_canceled IS FALSE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Most of them are cancelled by users, but the cancellation reason is unknown and there is no cancellation request date in the terms table.

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusions:** Exclude subscriptions that ended with no `cancel_requested_at` in the terms table, but have `is_failed_payment_canceled = FALSE`.
# MAGIC
# MAGIC These are not clean involuntary (delinquency) cases: the term ended without a cancel request, yet the payment-failure cancel flag is explicitly false, so they are ambiguous and should be left out of the voluntary / involuntary comparison.

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 2. Voluntary vs involuntary — plan type comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW subscription_terms_qualified_new_labeled_valid AS
# MAGIC SELECT
# MAGIC     l.*
# MAGIC FROM subscription_terms_qualified_new_labeled as l
# MAGIC join ${eda.terms_b} t
# MAGIC     ON l.subscription_id = t.subscription_id
# MAGIC -- Exclude ambiguous "cancelled by users but without cancel request" rows
# MAGIC WHERE NOT (
# MAGIC     t.cancel_requested_at IS NULL
# MAGIC     AND t.term_ended_at IS NOT NULL
# MAGIC     AND t.is_failed_payment_canceled IS FALSE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     pt.term_months                                              AS cadence,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY q.cancellation_type
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC    AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     pt.drug_name,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY q.cancellation_type
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC    AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     pt.regimen,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY q.cancellation_type
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC    AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     CASE
# MAGIC         WHEN pt.drug_strength IN ('2.5mg','5mg')         THEN 'low (≤5mg)'
# MAGIC         WHEN pt.drug_strength IN ('10mg','20mg','25mg')  THEN 'mid (10–25mg)'
# MAGIC         WHEN pt.drug_strength IN ('50mg','100mg')        THEN 'high (≥50mg)'
# MAGIC     END AS strength_group,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY q.cancellation_type
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC    AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     CASE
# MAGIC         WHEN pt.monthly_dose <= 8  THEN 'low (≤8/mo)'
# MAGIC         WHEN pt.monthly_dose <= 16 THEN 'mid (9–16/mo)'
# MAGIC         WHEN pt.monthly_dose = 30  THEN 'high (30/mo)'
# MAGIC     END AS dose_group,
# MAGIC     COUNT(DISTINCT q.subscription_id)                          AS n,
# MAGIC     ROUND(COUNT(DISTINCT q.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT q.subscription_id)) OVER (
# MAGIC               PARTITION BY q.cancellation_type
# MAGIC           ), 1) AS pct_within_type
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.plan_terms} pt
# MAGIC     ON q.subscription_term_id = pt.subscription_term_id
# MAGIC    AND pt.is_latest_plan_term = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** compared to voluntary cancellers, involuntary cancellers have shorter cadence (charged more frequently), higher dosage and strength (more expensive monthly)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 3. Voluntary vs involuntary — payment behavior

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Avg charge attempts, failure, and delinquency by cancellation type
# MAGIC SELECT
# MAGIC     q.cancellation_type,
# MAGIC     ROUND(AVG(inv.num_charges), 2)                                AS avg_num_charges,
# MAGIC     ROUND(AVG(CASE WHEN inv.is_failed THEN 1.0 ELSE 0 END), 3)   AS avg_failure_rate,
# MAGIC     ROUND(AVG(CASE WHEN inv.is_delinquent THEN 1.0 ELSE 0 END), 3) AS avg_delinquency_rate,
# MAGIC     COUNT(DISTINCT q.subscription_id)                            AS n
# MAGIC FROM subscription_terms_qualified_new_labeled_valid q
# MAGIC JOIN ${eda.invoices} inv
# MAGIC     ON q.subscription_term_id = inv.subscription_term_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 4. Voluntary vs involuntary — timing (days from term start to cancellation event)

# COMMAND ----------

df4 = spark.sql(f"""
    SELECT
        q.cancellation_type,
        DATEDIFF(
            DAY,
            t.term_started_at::date,
            COALESCE(t.cancel_requested_at::date, t.term_ended_at::date)
        ) AS days_to_cancellation
    FROM subscription_terms_qualified_new_labeled_valid q
    JOIN {TERMS_B} t ON q.subscription_term_id = t.subscription_term_id
    WHERE q.cancellation_type IN ('voluntary_cancellation', 'involuntary_cancellation')
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 5))
for cancellation_type, color in [
    ("voluntary_cancellation", "#E53935"),
    ("involuntary_cancellation", "#FF9800"),
]:
    vals = df4[df4["cancellation_type"] == cancellation_type]["days_to_cancellation"].dropna()
    ax.hist(
        vals,
        bins=60,
        alpha=0.5,
        color=color,
        label=f"{cancellation_type} (n={len(vals):,})",
        density=True,
    )
ax.set_xlabel("Days from term start to cancellation event")
ax.set_ylabel("Density")
ax.set_title("Cancellation Timing: Voluntary vs Involuntary", fontweight="bold")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 5. Cancellation label coverage — how much involuntary cancellation does the label capture?
# MAGIC
# MAGIC The model label (`cancel_status = 'cancelled_in_30_days'`) is based on `cancel_requested_at`
# MAGIC (voluntary cancellation). Involuntary cancellations typically do **not** submit a cancel request —
# MAGIC the term is ended by the system. This section measures the overlap.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     l.cancel_status,
# MAGIC     q.cancellation_type,
# MAGIC     COUNT(DISTINCT l.subscription_id) AS n,
# MAGIC     ROUND(COUNT(DISTINCT l.subscription_id) * 100.0
# MAGIC           / SUM(COUNT(DISTINCT l.subscription_id)) OVER (), 1) AS pct
# MAGIC FROM ${eda.labels} l
# MAGIC JOIN subscription_terms_qualified_new_labeled_valid q
# MAGIC     ON l.subscription_term_id = q.subscription_term_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:**
# MAGIC - `cancelled_in_30_days` + `voluntary_cancellation` → voluntary cancel correctly captured by label
# MAGIC - `cancelled_in_30_days` + `involuntary_cancellation` → should be rare / empty (label requires cancel request)
# MAGIC - `not_cancelled` (label) + `involuntary_cancellation` → involuntary cancellation not captured by the voluntary-cancel label (expected)
# MAGIC
# MAGIC **Findings:** []

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 6. Why the prevention model targets voluntary cancellation
# MAGIC
# MAGIC | | Voluntary cancellation | Involuntary cancellation |
# MAGIC | --- | --- | --- |
# MAGIC | Trigger | User decision (`cancel_requested_at`) | Term end without cancel request (e.g. delinquency) |
# MAGIC | Signal | Behavioral disengagement, dissatisfaction | Payment method issue, billing failure |
# MAGIC | Intervention | Retention offer, engagement campaign | Payment recovery, card update nudge |
# MAGIC | Model target | ✓ Cancel request within next 30 days | ✗ Not a voluntary cancel; appears as label=0 |
# MAGIC | Operational owner | Retention / growth | Payments / ops |
# MAGIC
# MAGIC **Conclusion:** The model is a *voluntary cancellation prevention* model. Involuntary
# MAGIC cancellations can still appear in weekly snapshots as non-events (label=0) with a stop rule
# MAGIC before term end, but the predicted outcome is user-initiated cancel — not payment failure.
# MAGIC Payment recovery should be handled separately.
