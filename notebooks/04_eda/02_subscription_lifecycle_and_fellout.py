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
SUBS      = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"
TERMS_B   = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"
TERMS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_all_terms"
INVOICES  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
EVENTS    = f"{CATALOG}.{SCHEMA}.ed_silver_subs_kafka__events"
ORDERS    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_orders"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"

spark.conf.set("eda.qual",    QUAL)
spark.conf.set("eda.subs_b",    SUBS_B)
spark.conf.set("eda.subs",    SUBS)
spark.conf.set("eda.terms_b",    TERMS_B)
spark.conf.set("eda.terms",    TERMS)
spark.conf.set("eda.invs",    INVOICES)
spark.conf.set("eda.events",  EVENTS)
spark.conf.set("eda.orders",  ORDERS)
spark.conf.set("eda.plan_terms", PLAN_TERMS)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Part A — Subscription Lifecycle

# COMMAND ----------

# MAGIC %md ## 1. Funnel: Signup → Wheel visit → Rx Written → Activated / Paid

# COMMAND ----------

# MAGIC %md
# MAGIC Note: all users who register and complete the intake form are considered to have a telehealth visit, since the visit consists of Licensed U.S. clinicians from Wheel reviewing the intake.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT s.subscription_id)                                           AS signup,
# MAGIC     -- Rx written: condition_subscription_prescription_written event
# MAGIC     SUM(CASE WHEN s.rx_is_written = TRUE THEN 1 ELSE 0 END)                     AS rx_written,
# MAGIC     ROUND(SUM(CASE WHEN s.rx_is_written = TRUE THEN 1.0 ELSE 0 END)
# MAGIC           / COUNT(DISTINCT s.subscription_id) * 100, 1)                         AS pct_rx_written,
# MAGIC     -- Paid
# MAGIC     SUM(CASE WHEN s.is_paid = TRUE THEN 1 ELSE 0 END)                           AS paid,
# MAGIC     ROUND(SUM(CASE WHEN s.is_paid = TRUE THEN 1.0 ELSE 0 END)
# MAGIC           / COUNT(DISTINCT s.subscription_id) * 100, 1)                         AS pct_paid,
# MAGIC     -- Activated
# MAGIC     SUM(CASE WHEN s.is_activated = TRUE THEN 1 ELSE 0 END)                      AS activated,
# MAGIC     ROUND(SUM(CASE WHEN s.is_activated = TRUE THEN 1.0 ELSE 0 END)
# MAGIC           / COUNT(DISTINCT s.subscription_id) * 100, 1)                         AS pct_activated
# MAGIC FROM ${eda.subs_b} s
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT subscription_id
# MAGIC     FROM ${eda.events}
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC ) e ON s.subscription_id = e.subscription_id

# COMMAND ----------

# MAGIC %md ## 2. Lifecycle stage

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
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md ## 3. Time from subscription signup to Rx written (days)

# COMMAND ----------

# MAGIC %md
# MAGIC The telehealth visit is very efficient—most users receive an Rx on the same day they sign up.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, s.created_at::date, e.occurred_at::date) AS days_to_rx,
# MAGIC     COUNT(*)                                               AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC JOIN (
# MAGIC     SELECT subscription_id, MIN(occurred_at) AS occurred_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC     GROUP BY 1
# MAGIC ) e ON s.subscription_id = e.subscription_id
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ## 4. Time from Rx written to activation (days)

# COMMAND ----------

# MAGIC %md
# MAGIC Subscribers pay immediately after the prescription is ready.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, e.occurred_at::date, s.activated_at::date) AS days_rx_to_activation,
# MAGIC     COUNT(*)                                                  AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)        AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC JOIN (
# MAGIC     SELECT subscription_id, MIN(occurred_at) AS occurred_at
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_silver_subs_kafka__events
# MAGIC     WHERE event_name = 'condition_subscription_prescription_written'
# MAGIC     GROUP BY 1
# MAGIC ) e ON s.subscription_id = e.subscription_id
# MAGIC WHERE s.is_activated = TRUE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Overall survival

# COMMAND ----------

# %pip install lifelines  # uncomment if lifelines is not installed on your cluster

from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt

CATALOG    = "general_scratch_catalog"
SCHEMA     = "general_scratch"
QUAL       = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_terms_qualified"
TERMS_B    = f"{CATALOG}.{SCHEMA}.ed_bronze_subscription_terms"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"

df = spark.sql(f"""
    SELECT
        pt.term_months AS cadence,
        DATEDIFF(DAY,
            t.term_started_at::date,
            COALESCE(t.cancel_requested_at::date, DATE '2026-07-02')
        ) / 30.0 AS duration_months,
        CASE WHEN t.cancel_requested_at IS NOT NULL THEN 1 ELSE 0 END AS cancelled
    FROM {QUAL} q
    JOIN {TERMS_B} t
        ON q.subscription_term_id = t.subscription_term_id
    JOIN {PLAN_TERMS} pt
        ON q.subscription_term_id = pt.subscription_term_id
        AND pt.is_latest_plan_term = TRUE
    WHERE t.cancel_requested_at IS NULL
       OR t.cancel_requested_at > t.term_started_at
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
kmf = KaplanMeierFitter()
colors = {1: "#2196F3", 3: "#FF9800", 6: "#4CAF50"}

for cadence, group in df.groupby("cadence"):
    kmf.fit(
        group["duration_months"],
        event_observed=group["cancelled"],
        label=f"{cadence}-month plan (n={len(group):,})"
    )
    kmf.plot_survival_function(ax=ax, color=colors.get(cadence, "gray"), ci_show=True)

ax.set_xlabel("Months since term start", fontsize=12)
ax.set_ylabel("Proportion not cancelled", fontsize=12)
ax.set_title("Survival Curve by Cadence", fontsize=14, fontweight="bold")
ax.set_xlim(0, 12)
ax.set_ylim(0.5, 1.0)
ax.axvline(x=1, color="gray", linestyle="--", alpha=0.5, label="Month 1")
ax.axvline(x=3, color="gray", linestyle=":",  alpha=0.5, label="Month 3")
ax.axvline(x=6, color="gray", linestyle="-.", alpha=0.5, label="Month 6")
ax.grid(True, alpha=0.3)
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

df = spark.sql(f"""
    SELECT
        pt.drug_name,
        DATEDIFF(DAY,
            t.term_started_at::date,
            COALESCE(t.cancel_requested_at::date, DATE '2026-07-02')
        ) / 30.0 AS duration_months,
        CASE WHEN t.cancel_requested_at IS NOT NULL THEN 1 ELSE 0 END AS cancelled
    FROM {QUAL} q
    JOIN {TERMS_B} t
        ON q.subscription_term_id = t.subscription_term_id
    JOIN {PLAN_TERMS} pt
        ON q.subscription_term_id = pt.subscription_term_id
        AND pt.is_latest_plan_term = TRUE
    WHERE t.cancel_requested_at IS NULL
       OR t.cancel_requested_at > t.term_started_at
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
kmf = KaplanMeierFitter()
colors = {"sildenafil": "#2196F3", "tadalafil (cialis)": "#FF9800"}

for drug, group in df.groupby("drug_name"):
    kmf.fit(
        group["duration_months"],
        event_observed=group["cancelled"],
        label=f"{drug} (n={len(group):,})"
    )
    kmf.plot_survival_function(ax=ax, color=colors.get(drug, "gray"), ci_show=True)

ax.set_xlabel("Months since term start", fontsize=12)
ax.set_ylabel("Proportion not cancelled", fontsize=12)
ax.set_title("Survival Curve by Drug Name", fontsize=14, fontweight="bold")
ax.set_xlim(0, 12)
ax.set_ylim(0.5, 1.0)
ax.axvline(x=1, color="gray", linestyle="--", alpha=0.5, label="Month 1")
ax.axvline(x=3, color="gray", linestyle=":",  alpha=0.5, label="Month 3")
ax.axvline(x=6, color="gray", linestyle="-.", alpha=0.5, label="Month 6")
ax.grid(True, alpha=0.3)
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

df = spark.sql(f"""
    SELECT
        pt.regimen,
        DATEDIFF(DAY,
            t.term_started_at::date,
            COALESCE(t.cancel_requested_at::date, DATE '2026-07-02')
        ) / 30.0 AS duration_months,
        CASE WHEN t.cancel_requested_at IS NOT NULL THEN 1 ELSE 0 END AS cancelled
    FROM {QUAL} q
    JOIN {TERMS_B} t
        ON q.subscription_term_id = t.subscription_term_id
    JOIN {PLAN_TERMS} pt
        ON q.subscription_term_id = pt.subscription_term_id
        AND pt.is_latest_plan_term = TRUE
    WHERE t.cancel_requested_at IS NULL
       OR t.cancel_requested_at > t.term_started_at
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
kmf = KaplanMeierFitter()
colors = {"DAILY": "#4CAF50", "AS_NEEDED": "#2196F3"}

for regimen, group in df.groupby("regimen"):
    kmf.fit(
        group["duration_months"],
        event_observed=group["cancelled"],
        label=f"{regimen} (n={len(group):,})"
    )
    kmf.plot_survival_function(ax=ax, color=colors.get(regimen, "gray"), ci_show=True)

ax.set_xlabel("Months since term start", fontsize=12)
ax.set_ylabel("Proportion not cancelled", fontsize=12)
ax.set_title("Survival Curve by Regimen", fontsize=14, fontweight="bold")
ax.set_xlim(0, 12)
ax.set_ylim(0.5, 1.0)
ax.axvline(x=1, color="gray", linestyle="--", alpha=0.5, label="Month 1")
ax.axvline(x=3, color="gray", linestyle=":",  alpha=0.5, label="Month 3")
ax.axvline(x=6, color="gray", linestyle="-.", alpha=0.5, label="Month 6")
ax.grid(True, alpha=0.3)
ax.legend(fontsize=10)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md ## 6. Overall cancellation timing — time from activation to cancel request (days)

# COMMAND ----------

# MAGIC %md
# MAGIC Cancellation peaks on day 0, day 23, and day 83 after subscription activation.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATEDIFF(DAY, s.activated_at::date, t.cancel_requested_at::date) AS days_to_cancel,
# MAGIC     COUNT(*)                                                           AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)                AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON t.subscription_id = s.subscription_id
# MAGIC WHERE t.cancel_requested_at IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC Because subscription periods vary, it is important to analyze differences by cadence. However, users can change their period length over time, so this analysis **includes only those who never change their plan**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW one_plan_subs AS
# MAGIC     select
# MAGIC     subscription_id,
# MAGIC     term_months
# MAGIC     from general_scratch_catalog.general_scratch.   ed_bronze_subscription_plan_terms
# MAGIC     where subscription_id in (
# MAGIC         select
# MAGIC             subscription_id
# MAGIC         from general_scratch_catalog.general_scratch.   ed_bronze_subscription_plan_terms
# MAGIC         group by 1
# MAGIC         having count(distinct subscription_plan_term_id) = 1
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW days_to_cancel_by_plan_month AS
# MAGIC     select
# MAGIC     o.term_months,
# MAGIC     DATEDIFF(DAY, s.activated_at::date, t.cancel_requested_at::date) AS days_to_cancel,
# MAGIC     COUNT(*)                                                           AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)                AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscription_terms t
# MAGIC JOIN general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     ON t.subscription_id = s.subscription_id
# MAGIC join one_plan_subs o
# MAGIC     on s.subscription_id = o.subscription_id
# MAGIC WHERE t.cancel_requested_at IS NOT NULL
# MAGIC group by 1,2
# MAGIC order by 1 asc, 3 desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Cadence = 1 month

# COMMAND ----------

# MAGIC %md
# MAGIC 1-month plan users are more likely to cancel on day 0 and day 23 - when they receive a refill reminder (Conditions subs users receive the upcoming refill / next-fill reminder when a fill is due within the next 7 days.)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from days_to_cancel_by_plan_month
# MAGIC where term_months = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Cadence = 3 month

# COMMAND ----------

# MAGIC %md
# MAGIC The pattern is similar to the 1-month plan, with cancellation peaking on day 0 and day 83. The difference is that most users cancel on day 83 instead of day 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from days_to_cancel_by_plan_month
# MAGIC where term_months = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Cadence = 6 month

# COMMAND ----------

# MAGIC %md
# MAGIC Cancellation spikes on day 0, with a large number of users also canceling during the first week and on day 173.

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from days_to_cancel_by_plan_month
# MAGIC where term_months = 6

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Part B — Fell-Out Analysis

# COMMAND ----------

# MAGIC %md ## 7. What plans do fell-out subscribers would like to have?

# COMMAND ----------

# MAGIC %md
# MAGIC More fellout users need Sildenafil compared to activated users. As Sildenafil is only taken as needed, it indicates fellout users prefer something as-needed and works quickly

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW first_plan_activated AS
# MAGIC     select
# MAGIC         s.subscription_id,
# MAGIC         s.is_activated,
# MAGIC         pt.drug_name,
# MAGIC         -- pt.drug_strength,
# MAGIC         pt.regimen,
# MAGIC         pt.term_months,
# MAGIC         pt.monthly_dose
# MAGIC     from general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     join general_scratch_catalog.general_scratch.  ed_bronze_subscription_plan_terms pt
# MAGIC     on s.subscription_id = pt.subscription_id
# MAGIC     where pt.term_number = 1
# MAGIC         and pt.plan_term_number = 1
# MAGIC         and s.is_activated = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW first_plan_fellout AS
# MAGIC     SELECT
# MAGIC         s.subscription_id,
# MAGIC         s.is_activated,
# MAGIC         pt.drug_name,
# MAGIC         s.current_regimen as regimen,
# MAGIC         case when s.current_days_supply in (30, 32) then 1
# MAGIC             when s.current_days_supply in (90,96) then 3
# MAGIC             when s.current_days_supply in (180, 192) then 6
# MAGIC             else null end as term_months,
# MAGIC         s.current_quantity/term_months as monthly_dose
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     JOIN general_scratch_catalog.general_scratch.  ed_silver_subscription_plan_types pt
# MAGIC         ON s.current_drug_id = pt.drug_id
# MAGIC     WHERE s.is_activated = FALSE

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Drug name
# MAGIC
# MAGIC Compared with activated subscribers, fell-out subscribers are more likely to be on Sildenafil.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     case when is_activated is FALSE then 'fellout' else 'activated' end as group,
# MAGIC     drug_name,
# MAGIC     COUNT(DISTINCT subscription_id)  AS n,
# MAGIC     ROUND(COUNT(DISTINCT subscription_id) * 100.0 / SUM(COUNT(DISTINCT subscription_id)) OVER (partition by case when is_activated is FALSE then 'fellout' else 'activated' end), 1) AS pct
# MAGIC FROM (
# MAGIC     SELECT * FROM first_plan_activated
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM first_plan_fellout
# MAGIC ) combined
# MAGIC GROUP BY is_activated, drug_name
# MAGIC ORDER BY 1,3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Regimen

# COMMAND ----------

# MAGIC %md
# MAGIC Compared with activated subscribers, fell-out subscribers are more likely to be on as-needed plans.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     case when is_activated is FALSE then 'fellout' else 'activated' end as group,
# MAGIC     regimen,
# MAGIC     COUNT(DISTINCT subscription_id)  AS n,
# MAGIC     ROUND(COUNT(DISTINCT subscription_id) * 100.0 / SUM(COUNT(DISTINCT subscription_id)) OVER (partition by case when is_activated is FALSE then 'fellout' else 'activated' end), 1) AS pct
# MAGIC FROM (
# MAGIC     SELECT * FROM first_plan_activated
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM first_plan_fellout
# MAGIC ) combined
# MAGIC GROUP BY is_activated, regimen
# MAGIC ORDER BY 1,3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Cadence

# COMMAND ----------

# MAGIC %md
# MAGIC Compared with activated subscribers, fell-out subscribers are more likely to be short-term plans.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     case when is_activated is FALSE then 'fellout' else 'activated' end as group,
# MAGIC     term_months,
# MAGIC     COUNT(DISTINCT subscription_id)  AS n,
# MAGIC     ROUND(COUNT(DISTINCT subscription_id) * 100.0 / SUM(COUNT(DISTINCT subscription_id)) OVER (partition by case when is_activated is FALSE then 'fellout' else 'activated' end), 1) AS pct
# MAGIC FROM (
# MAGIC     SELECT * FROM first_plan_activated
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM first_plan_fellout
# MAGIC ) combined
# MAGIC GROUP BY is_activated, term_months
# MAGIC ORDER BY 1,3 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Monthly doses

# COMMAND ----------

# MAGIC %md
# MAGIC Compared with activated subscribers, fell-out subscribers are more likely to have lower monthly dose.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     case when is_activated is FALSE then 'fellout' else 'activated' end as group,
# MAGIC     monthly_dose,
# MAGIC     COUNT(DISTINCT subscription_id)  AS n,
# MAGIC     ROUND(COUNT(DISTINCT subscription_id) * 100.0 / SUM(COUNT(DISTINCT subscription_id)) OVER (partition by case when is_activated is FALSE then 'fellout' else 'activated' end), 1) AS pct
# MAGIC FROM (
# MAGIC     SELECT * FROM first_plan_activated
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM first_plan_fellout
# MAGIC ) combined
# MAGIC GROUP BY is_activated, monthly_dose
# MAGIC ORDER BY 1,3 DESC

# COMMAND ----------

# MAGIC %md ## 8. Fell-out subscribers — did they have an Rx written?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE WHEN s.rx_is_written THEN 'rx_written' ELSE 'no_rx' END AS rx_status,
# MAGIC     COUNT(*)                                                AS n,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     WHERE s.is_activated = FALSE
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC Most subscribers do not have an Rx written, so they likely dropped out because they were ineligible.

# COMMAND ----------

# MAGIC %md ## 9. Fell-out subscribers — did they have an order placed?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH fell_out AS (
# MAGIC     SELECT s.subscription_id
# MAGIC     FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC     WHERE s.is_activated = FALSE
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

# MAGIC %md ## 10. Comparison: fell-out vs activated — acquisition channel and platform

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
# MAGIC FROM general_scratch_catalog.general_scratch.ed_bronze_subscriptions s
# MAGIC GROUP BY CASE WHEN s.is_activated = TRUE THEN 'activated' ELSE 'fell_out' END,2,3
# MAGIC ORDER BY 1, 4 DESC
