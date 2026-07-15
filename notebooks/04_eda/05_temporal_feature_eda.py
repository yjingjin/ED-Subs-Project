# Databricks notebook source
# 04 — Temporal Behavior EDA (Phase 2)
#
# Prerequisites: ed_silver_subscription_weekly_snapshots (build_silver.py § 11).
#
# Parts:
#   A. Temporal behavior — feature value by week number (14-day look-back), churners vs retained
#      Line plots for binary/proportion features; boxplots/violins for count/continuous features.
#   B. Look-back window selection — chi-square by feature × window (7, 14, 30 days)

# COMMAND ----------

CATALOG    = "general_scratch_catalog"
SCHEMA     = "general_scratch"

SNAPSHOTS  = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_weekly_snapshots"
INVOICES   = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_invoices"
EVENTS     = f"{CATALOG}.{SCHEMA}.ed_silver_subs_kafka__events"
PLAN_TERMS = f"{CATALOG}.{SCHEMA}.ed_silver_subscription_plan_terms"
SUBS       = f"{CATALOG}.{SCHEMA}.ed_silver_subscriptions"

spark.conf.set("eda.snapshots",  SNAPSHOTS)
spark.conf.set("eda.invoices",   INVOICES)
spark.conf.set("eda.events",     EVENTS)
spark.conf.set("eda.plan_terms", PLAN_TERMS)
spark.conf.set("eda.subs",       SUBS)

from scipy.stats import chi2_contingency, mannwhitneyu
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

WINDOW = 14   # look-back window in days for Part A
MAX_WEEKS = 26

def line_plot(df, x_col, y_col, ylabel, title, pct=False):
    fig, ax = plt.subplots(figsize=(12, 4))
    for grp, color in [("churned", "#E53935"), ("retained", "#1E88E5")]:
        d = df[df["group"] == grp]
        ax.plot(d[x_col], d[y_col], label=grp, color=color, linewidth=2, marker="o", markersize=4)
    ax.set_xlabel("Week since term start"); ax.set_ylabel(ylabel)
    ax.set_title(title, fontweight="bold")
    if pct: ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
    ax.legend(); ax.grid(True, alpha=0.3); plt.tight_layout(); plt.show()

def box_violin(df, feature_col, title, log_scale=False):
    churned  = df[df["label"] == 1][feature_col].dropna()
    retained = df[df["label"] == 0][feature_col].dropna()
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    # Boxplot
    axes[0].boxplot([retained, churned], labels=["retained", "churned"],
                    patch_artist=True,
                    boxprops=dict(facecolor="#1E88E5", alpha=0.5),
                    medianprops=dict(color="black"))
    axes[0].boxplot([churned], positions=[2], labels=["churned"],
                    patch_artist=True,
                    boxprops=dict(facecolor="#E53935", alpha=0.5),
                    medianprops=dict(color="black"))
    axes[0].set_title(f"{title} — Boxplot"); axes[0].grid(True, alpha=0.3)
    if log_scale: axes[0].set_yscale("log")
    # Violin
    parts = axes[1].violinplot([retained, churned], positions=[1, 2], showmedians=True)
    for pc, color in zip(parts["bodies"], ["#1E88E5", "#E53935"]):
        pc.set_facecolor(color); pc.set_alpha(0.5)
    axes[1].set_xticks([1, 2]); axes[1].set_xticklabels(["retained", "churned"])
    axes[1].set_title(f"{title} — Violin"); axes[1].grid(True, alpha=0.3)
    if log_scale: axes[1].set_yscale("log")
    plt.tight_layout(); plt.show()
    stat, p = mannwhitneyu(churned, retained, alternative="two-sided")
    sig = "✓ Significant" if p < 0.05 else "✗ Not significant"
    print(f"Mann-Whitney U | p={p:.4f} | {sig}")

def density_plot(df, feature_col, title):
    from scipy.stats import gaussian_kde
    fig, ax = plt.subplots(figsize=(10, 4))
    for label, color, name in [(1, "#E53935", "churned"), (0, "#1E88E5", "retained")]:
        vals = df[df["label"] == label][feature_col].dropna()
        if len(vals) > 1:
            kde = gaussian_kde(vals)
            x = np.linspace(vals.min(), vals.max(), 300)
            ax.plot(x, kde(x), label=name, color=color, linewidth=2)
            ax.fill_between(x, kde(x), alpha=0.15, color=color)
    ax.set_xlabel(feature_col); ax.set_ylabel("Density")
    ax.set_title(title, fontweight="bold"); ax.legend(); ax.grid(True, alpha=0.3)
    plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Part A — Temporal behavior by week (14-day look-back window)

# COMMAND ----------
# MAGIC %md ### A1. Invoice: failure rate

# COMMAND ----------

df_a1_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN inv.is_failed THEN 1.0 ELSE 0.0 END) AS val
    FROM {SNAPSHOTS} s
    JOIN {INVOICES} inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.created_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a1_line, "week_number", "val",
          f"Avg failure rate ({WINDOW}-day window)", "Invoice Failure Rate by Week", pct=True)

# COMMAND ----------
# MAGIC %md ### A2. Invoice: delinquency rate

# COMMAND ----------

df_a2_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN inv.is_delinquent THEN 1.0 ELSE 0.0 END) AS val
    FROM {SNAPSHOTS} s
    JOIN {INVOICES} inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.created_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a2_line, "week_number", "val",
          f"Avg delinquency rate ({WINDOW}-day window)", "Delinquency Rate by Week", pct=True)

# COMMAND ----------
# MAGIC %md ### A3. Invoice: number of fills and refills

# COMMAND ----------

df_a3_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(num_fills) AS avg_fills,
           AVG(GREATEST(num_fills - 1, 0)) AS avg_refills
    FROM {SNAPSHOTS} s
    JOIN (
        SELECT subscription_term_id, created_at::date AS d,
               SUM(CASE WHEN is_paid THEN 1 ELSE 0 END) AS num_fills
        FROM {INVOICES} GROUP BY 1,2
    ) inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.d BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a3_line, "week_number", "avg_fills",
          f"Avg fills ({WINDOW}-day window)", "Number of Fills by Week")
line_plot(df_a3_line, "week_number", "avg_refills",
          f"Avg refills ({WINDOW}-day window)", "Number of Refills by Week")

# COMMAND ----------
# MAGIC %md #### Distribution: fills and refills (all snapshots)

# COMMAND ----------

df_a3_dist = spark.sql(f"""
    SELECT s.label, SUM(CASE WHEN inv.is_paid THEN 1 ELSE 0 END) AS num_fills
    FROM {SNAPSHOTS} s
    JOIN {INVOICES} inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.created_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    GROUP BY s.subscription_id, s.snapshot_date, s.label
""").toPandas()
box_violin(df_a3_dist, "num_fills", "Number of Fills")

# COMMAND ----------
# MAGIC %md ### A4. Invoice: refund count

# COMMAND ----------

df_a4_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN inv.is_refunded THEN 1.0 ELSE 0.0 END) AS val
    FROM {SNAPSHOTS} s
    JOIN {INVOICES} inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.created_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a4_line, "week_number", "val",
          f"Avg refund rate ({WINDOW}-day window)", "Refund Rate by Week", pct=True)

# COMMAND ----------
# MAGIC %md ### A5. Invoice: number of charges

# COMMAND ----------

df_a5_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(inv.num_charges) AS val
    FROM {SNAPSHOTS} s
    JOIN {INVOICES} inv ON s.subscription_term_id = inv.subscription_term_id
        AND inv.created_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a5_line, "week_number", "val",
          f"Avg num_charges per invoice ({WINDOW}-day window)", "Number of Charges by Week")

# COMMAND ----------
# MAGIC %md ### A6. Deferral: proportion deferred, count, total days deferred

# COMMAND ----------

df_a6_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN e.subscription_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS has_deferred,
           AVG(COALESCE(e.num_deferrals, 0))                                   AS avg_num_deferrals,
           AVG(COALESCE(e.total_days_deferred, 0))                             AS avg_days_deferred
    FROM {SNAPSHOTS} s
    LEFT JOIN (
        SELECT subscription_id,
               occurred_at::date AS d,
               COUNT(*) AS num_deferrals,
               SUM(DATEDIFF(DAY, old_renewal_at::date, new_renewal_at::date)) AS total_days_deferred
        FROM {EVENTS}
        WHERE event_name = 'term_renewal_time_changed'
          AND changed_by = 'CHANGED_BY_USER'
          AND old_renewal_at IS NOT NULL AND new_renewal_at IS NOT NULL
        GROUP BY 1,2
    ) e ON s.subscription_id = e.subscription_id
        AND e.d BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()

line_plot(df_a6_line, "week_number", "has_deferred",
          f"Proportion with deferral ({WINDOW}-day)", "Deferral Rate by Week", pct=True)
line_plot(df_a6_line, "week_number", "avg_num_deferrals",
          f"Avg deferral count ({WINDOW}-day)", "Number of Deferrals by Week")
line_plot(df_a6_line, "week_number", "avg_days_deferred",
          f"Avg total days deferred ({WINDOW}-day)", "Total Days Deferred by Week")

# COMMAND ----------
# MAGIC %md #### Distribution: total days deferred (all snapshots)

# COMMAND ----------

df_a6_dist = spark.sql(f"""
    SELECT s.label,
           COALESCE(SUM(DATEDIFF(DAY, e.old_renewal_at::date, e.new_renewal_at::date)), 0) AS total_days_deferred
    FROM {SNAPSHOTS} s
    LEFT JOIN {EVENTS} e
        ON s.subscription_id = e.subscription_id
        AND e.event_name = 'term_renewal_time_changed'
        AND e.changed_by = 'CHANGED_BY_USER'
        AND e.old_renewal_at IS NOT NULL AND e.new_renewal_at IS NOT NULL
        AND e.occurred_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    GROUP BY s.subscription_id, s.snapshot_date, s.label
""").toPandas()
density_plot(df_a6_dist[df_a6_dist["total_days_deferred"] > 0],
             "total_days_deferred", "Total Days Deferred Distribution (subscribers who deferred)")

# COMMAND ----------
# MAGIC %md ### A7. Refill reminder events

# COMMAND ----------

df_a7_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN e.subscription_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS has_reminder,
           AVG(COALESCE(e.n_reminders, 0)) AS avg_reminders
    FROM {SNAPSHOTS} s
    LEFT JOIN (
        SELECT subscription_id, occurred_at::date AS d, COUNT(*) AS n_reminders
        FROM {EVENTS}
        WHERE event_name = 'upcoming_term_renewal_notified'
        GROUP BY 1,2
    ) e ON s.subscription_id = e.subscription_id
        AND e.d BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()
line_plot(df_a7_line, "week_number", "has_reminder",
          f"Proportion with refill reminder ({WINDOW}-day)", "Refill Reminder Rate by Week", pct=True)

# COMMAND ----------
# MAGIC %md ### A8. Plan changes by type

# COMMAND ----------

df_a8_line = spark.sql(f"""
    SELECT s.week_number,
           CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
           AVG(CASE WHEN pt.plan_term_number > 1 THEN 1.0 ELSE 0.0 END) AS has_plan_change,
           AVG(CASE WHEN pt.plan_term_number > 1
                     AND pt.drug_name != LAG(pt.drug_name) IGNORE NULLS OVER (
                         PARTITION BY pt.subscription_term_id ORDER BY pt.plan_term_started_at)
                    THEN 1.0 ELSE 0.0 END)  AS drug_change,
           AVG(CASE WHEN pt.plan_term_number > 1
                     AND pt.regimen != LAG(pt.regimen) IGNORE NULLS OVER (
                         PARTITION BY pt.subscription_term_id ORDER BY pt.plan_term_started_at)
                    THEN 1.0 ELSE 0.0 END)  AS regimen_change,
           AVG(CASE WHEN pt.plan_term_number > 1
                     AND pt.drug_strength != LAG(pt.drug_strength) IGNORE NULLS OVER (
                         PARTITION BY pt.subscription_term_id ORDER BY pt.plan_term_started_at)
                    THEN 1.0 ELSE 0.0 END)  AS strength_change,
           AVG(CASE WHEN pt.plan_term_number > 1
                     AND pt.monthly_dose != LAG(pt.monthly_dose) IGNORE NULLS OVER (
                         PARTITION BY pt.subscription_term_id ORDER BY pt.plan_term_started_at)
                    THEN 1.0 ELSE 0.0 END)  AS dose_change,
           AVG(CASE WHEN pt.plan_term_number > 1
                     AND pt.starting_fulfillment_type != LAG(pt.starting_fulfillment_type) IGNORE NULLS OVER (
                         PARTITION BY pt.subscription_term_id ORDER BY pt.plan_term_started_at)
                    THEN 1.0 ELSE 0.0 END)  AS fulfillment_change
    FROM {SNAPSHOTS} s
    JOIN {PLAN_TERMS} pt ON s.subscription_term_id = pt.subscription_term_id
        AND pt.plan_term_started_at::date BETWEEN DATEADD(DAY,-{WINDOW},s.snapshot_date) AND s.snapshot_date
    WHERE s.week_number BETWEEN 0 AND {MAX_WEEKS}
    GROUP BY 1,2 ORDER BY 1,2
""").toPandas()

for col, label in [
    ("has_plan_change", "Any Plan Change"),
    ("drug_change",     "Drug Change"),
    ("regimen_change",  "Regimen Change"),
    ("strength_change", "Strength Change"),
    ("dose_change",     "Dose Change"),
    ("fulfillment_change", "Fulfillment Type Change"),
]:
    line_plot(df_a8_line, "week_number", col,
              f"Proportion ({WINDOW}-day)", f"{label} Rate by Week", pct=True)

# COMMAND ----------
# MAGIC %md ### A9. Static features: current plan type and acquisition channel
# MAGIC
# MAGIC Static features do not vary by week — temporal plots are not applicable.
# MAGIC See EDA 03 (03_cancellation_types_eda.py) for cancellation rate by:
# MAGIC   cadence, drug name, regimen, strength group, dose group, acquisition channel.
# MAGIC
# MAGIC Below: distributions across the snapshot dataset for reference.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
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
# MAGIC     pt.regimen,
# MAGIC     CASE
# MAGIC         WHEN s_sub.first_channel_grouping IN ('organic search','paid search') THEN 'active_search'
# MAGIC         WHEN s_sub.first_channel_grouping IN ('direct','crm')                THEN 're_engaged'
# MAGIC         ELSE 'unknown_other'
# MAGIC     END AS channel_group,
# MAGIC     CASE WHEN s.label=1 THEN 'churned' ELSE 'retained' END AS group,
# MAGIC     COUNT(DISTINCT s.subscription_id) AS n
# MAGIC FROM ${eda.snapshots} s
# MAGIC JOIN ${eda.plan_terms} pt ON s.subscription_term_id = pt.subscription_term_id
#MAGIC     AND pt.is_latest_plan_term = TRUE
# MAGIC JOIN ${eda.subs} s_sub ON s.subscription_id = s_sub.subscription_id
# MAGIC GROUP BY 1,2,3,4,5

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Part B — Look-back window selection (chi-square)

# COMMAND ----------

chi2_results = []

feature_queries = {
    "invoice_failure": lambda w: f"""
        SELECT s.subscription_id, s.label,
               MAX(CASE WHEN inv.is_failed THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        JOIN {INVOICES} inv ON s.subscription_term_id=inv.subscription_term_id
            AND inv.created_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "delinquency": lambda w: f"""
        SELECT s.subscription_id, s.label,
               MAX(CASE WHEN inv.is_delinquent THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        JOIN {INVOICES} inv ON s.subscription_term_id=inv.subscription_term_id
            AND inv.created_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "num_fills": lambda w: f"""
        SELECT s.subscription_id, s.label,
               SUM(CASE WHEN inv.is_paid THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        JOIN {INVOICES} inv ON s.subscription_term_id=inv.subscription_term_id
            AND inv.created_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "refund_count": lambda w: f"""
        SELECT s.subscription_id, s.label,
               SUM(CASE WHEN inv.is_refunded THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        JOIN {INVOICES} inv ON s.subscription_term_id=inv.subscription_term_id
            AND inv.created_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "num_charges": lambda w: f"""
        SELECT s.subscription_id, s.label,
               SUM(inv.num_charges) AS val
        FROM {SNAPSHOTS} s
        JOIN {INVOICES} inv ON s.subscription_term_id=inv.subscription_term_id
            AND inv.created_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "has_deferral": lambda w: f"""
        SELECT s.subscription_id, s.label,
               MAX(CASE WHEN e.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        LEFT JOIN {EVENTS} e ON s.subscription_id=e.subscription_id
            AND e.event_name='term_renewal_time_changed' AND e.changed_by='CHANGED_BY_USER'
            AND e.occurred_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "refill_reminder": lambda w: f"""
        SELECT s.subscription_id, s.label,
               MAX(CASE WHEN e.subscription_id IS NOT NULL THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        LEFT JOIN {EVENTS} e ON s.subscription_id=e.subscription_id
            AND e.event_name='upcoming_term_renewal_notified'
            AND e.occurred_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
    "has_plan_change": lambda w: f"""
        SELECT s.subscription_id, s.label,
               MAX(CASE WHEN pt.plan_term_number > 1 THEN 1 ELSE 0 END) AS val
        FROM {SNAPSHOTS} s
        JOIN {PLAN_TERMS} pt ON s.subscription_term_id=pt.subscription_term_id
            AND pt.plan_term_started_at::date BETWEEN DATEADD(DAY,-{w},s.snapshot_date) AND s.snapshot_date
        GROUP BY 1,2""",
}

for feat_name, query_fn in feature_queries.items():
    for window in [7, 14, 30]:
        df = spark.sql(query_fn(window)).toPandas()
        df["binary_val"] = (df["val"] > 0).astype(int)
        table = [[((df["binary_val"]==v) & (df["label"]==c)).sum()
                  for c in [0,1]] for v in [0,1]]
        chi2, p, _, _ = chi2_contingency(table)
        chi2_results.append({"feature": feat_name, "window_days": window,
                              "chi2": round(chi2,2), "p_value": round(p,4),
                              "significant": p < 0.05})

summary = pd.DataFrame(chi2_results)
pivot = summary.pivot(index="feature", columns="window_days", values="chi2").round(2)
pivot.columns = [f"{c}d" for c in pivot.columns]
pivot["best_window"] = summary.groupby("feature").apply(
    lambda g: f"{g.loc[g['chi2'].idxmax(), 'window_days']}d"
).values

print("Chi-square by feature × look-back window (higher = more predictive)")
print("=" * 65)
print(pivot.sort_values("best_window").to_string())
