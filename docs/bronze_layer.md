# Bronze Layer — Source Filters & Ingestion Notes

All bronze tables are loaded from CSVs exported from Redshift (`goodrx_dbt` schema) via DataGrip
and uploaded to the Databricks Volume at:
`/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads/`

**Data pull date: July 2, 2026** — all tables were exported from Redshift on this date.

Bronze tables are raw and unmodified except for three metadata columns added at load time:
`_ingested_at`, `_source_file`, `_source_table`. All columns are typed (see `load_bronze.py`).

> **Scope note:** All bronze tables except `subscription_plan_types` and `int_subs_kafka__events`
> are filtered to ED subscriptions only at export (via `condition_name`, `subscription_subcategory`,
> or `subscription_name`). This means the qualified cohort and all silver tables include unpaid and
> unactivated ED subscriptions — useful for EDA. For labeling and modeling, an explicit filter to
> activated subs (`is_activated = TRUE`) is applied in `ed_silver_subscription_term_start_labels`
> via a join to `ed_silver_subscriptions` filtered on `subs_is_activated = TRUE`.

---

## Filters applied at export (Redshift → CSV)


| Table                               | WHERE clause                                                                                                                       | Export date |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `ed_bronze_subscriptions`           | `condition_name = 'erectile dysfunction'`                                                                                          | 07/02/2026  |
| `ed_bronze_subscription_terms`      | `LOWER(condition_name) = 'erectile dysfunction'`                                                                                   | 07/02/2026  |
| `ed_bronze_subscription_plan_terms` | `LOWER(condition_name) = 'erectile dysfunction'`                                                                                   | 07/02/2026  |
| `ed_bronze_subscription_charges`    | `LOWER(condition_name) = 'erectile dysfunction'`                                                                                   | 07/02/2026  |
| `ed_bronze_subscription_invoices`   | `LOWER(subscription_subcategory) = 'erectile dysfunction'`                                                                         | 07/02/2026  |
| `ed_bronze_subscriptions_churn`     | `LOWER(subscription_name) = 'conditions: erectile dysfunction'`                                                                    | 07/02/2026  |
| `ed_bronze_subscription_plan_types` | No filter — full reference table                                                                                                   | 07/02/2026  |
| `ed_bronze_int_subs_kafka__events`  | `LOWER(condition_name) = 'erectile dysfunction' AND event_name IN ('upcoming_term_renewal_notified', 'term_renewal_time_changed')` | 07/02/2026  |
| `ed_bronze_subscription_orders`     | `JOIN subscription_invoices ON latest_order_id = order_id WHERE LOWER(subscription_subcategory) = 'erectile dysfunction'`          | 07/02/2026  |


> **`int_subs_kafka__events`** — SQL file updated (`int_subs_kafka__events_07022026.sql`).

---

## Source SQL files

| Table | Source SQL file |
|---|---|
| `subscriptions` | `subscriptions_07022026.sql` |
| `subscription_terms` | `subscription_terms_07022026.sql` |
| `subscription_plan_terms` | `subscription_plan_terms_07022026.sql` |
| `subscription_charges` | `subscription_charges_07022026.sql` |
| `subscription_invoices` | `subscription_invoices_07022026.sql` |
| `subscriptions_churn` | `subscriptions_churn_07022026.sql` |
| `subscription_plan_types` | `subscription_plan_types_07022026.sql` |
| `int_subs_kafka__events` | `int_subs_kafka__events_07022026.sql` |
| `subscription_orders` | `subscription_orders_07022026.sql` |


---

## Ingestion notebook

`notebooks/00_ingestion/load_bronze.py`

Re-running is safe (overwrite mode). All loaded tables are processed in one run.