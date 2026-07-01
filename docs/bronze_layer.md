# Bronze Layer — Source Filters & Ingestion Notes

All bronze tables are loaded from CSVs exported from Redshift (`goodrx_dbt` schema) via DataGrip
and uploaded to the Databricks Volume at:
`/Volumes/general_scratch_catalog/general_scratch/checkpoints/jiny/ed_subs_raw_uploads/`

**Data pull date: June 30, 2026** — all tables were exported from Redshift on this date.

Bronze tables are raw and unmodified except for three metadata columns added at load time:
`_ingested_at`, `_source_file`, `_source_table`. All columns are typed (see `load_bronze.py`).

---

## Filters applied at export (Redshift → CSV)

| Table | WHERE clause | Export date |
|---|---|---|
| `ed_bronze_subscriptions` | `condition_name = 'erectile dysfunction' AND is_paid IS TRUE AND is_activated IS TRUE` | 06/30/2026 |
| `ed_bronze_subscription_terms` | `LOWER(condition_name) = 'erectile dysfunction' AND is_paid IS TRUE` | 06/30/2026 |
| `ed_bronze_subscription_plan_terms` | `LOWER(condition_name) = 'erectile dysfunction'` | 06/30/2026 |
| `ed_bronze_subscription_charges` | `LOWER(condition_name) = 'erectile dysfunction'` | 06/30/2026 |
| `ed_bronze_subscription_invoices` | `LOWER(subscription_subcategory) = 'erectile dysfunction'` | 06/30/2026 |
| `ed_bronze_subscriptions_churn` | `LOWER(subscription_name) = 'conditions: erectile dysfunction'` | 06/30/2026 |
| `ed_bronze_subscription_plan_types` | No filter — full reference table | 06/30/2026 |
| `ed_bronze_int_subs_kafka__events` | `LOWER(condition_name) = 'erectile dysfunction' AND event_name IN ('upcoming_term_renewal_notified', 'term_renewal_time_changed')` | 07/01/2026 |
| `ed_bronze_subscription_orders` | `JOIN subscription_invoices ON latest_order_id = order_id WHERE LOWER(subscription_subcategory) = 'erectile dysfunction'` | 07/01/2026 |

> **`int_subs_kafka__events`** — SQL file updated (`int_subs_kafka__events_06302026.sql`) but this
> table has not yet been added to `load_bronze.py`.

---

## Source SQL files

| Table | Source SQL file |
|---|---|
| `subscriptions` | `subscriptions_06302026.sql` |
| `subscription_terms` | `subscription_terms_06302026.sql` |
| `subscription_plan_terms` | `subscription_plan_terms_06302026.sql` |
| `subscription_charges` | `subscription_charges_06302026.sql` |
| `subscription_invoices` | `subscription_invoices_06302026.sql` |
| `subscriptions_churn` | `subscriptions_churn_06302026.sql` |
| `subscription_plan_types` | `subscription_plan_types_06302026.sql` |
| `int_subs_kafka__events` | `int_subs_kafka__events_07012026.sql` |
| `subscription_orders` | `subscription_orders_07012026.sql` |

---

## Ingestion notebook

`notebooks/00_ingestion/load_bronze.py`

Re-running is safe (overwrite mode). All loaded tables are processed in one run.
