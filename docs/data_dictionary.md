# Data dictionary

Source schema: **goodrx_dbt** (Redshift, read-only).  
All tables are ED-filtered at export (condition_id = 135 / subscription_name LIKE '%erectile dysfunction%').

---

## subscriptions

Core subscription record. One row per subscription.

> **Bronze filter applied at export (06/22/2026):**
> `condition_name = 'erectile dysfunction' AND is_paid IS TRUE AND is_activated IS TRUE`
> Source query: `sql/redshift/exports/goodrx_dbt/subscriptions_06222026.sql`

| Column | Type | Description |
|---|---|---|
| subscription_id | string (UUID) | Primary key |
| common_id | string (UUID) | Cross-table join key |
| created_at | timestamp | When the subscription was created |
| status | string | Current status: `active`, `canceled`, `paused`, … |
| raw_subscription_type | string | Raw type from source system |
| condition_id | int | 135 = erectile dysfunction |
| condition_name | string | "erectile dysfunction" |
| tenant_id | string | Tenant (e.g. "gdrx") |
| current_term_end | timestamp | End of current billing term |
| latest_delinquent_at | timestamp | Last time subscription became delinquent |
| latest_canceled_at | timestamp | Last cancellation timestamp |
| latest_paused_at | timestamp | Last pause timestamp |
| current_price | decimal | Current monthly price |
| current_drug_id | int | Drug being dispensed |
| current_regimen | string | Dosing regimen (e.g. AS_NEEDED, DAILY) |
| current_quantity | int | Quantity per fill |
| current_fulfillment_method | string | Delivery method |
| current_days_supply | int | Days supply per fill |
| had_trial | bool | Whether the subscription started with a trial |
| is_paid | bool | Whether at least one payment has been made |
| is_activated | bool | Whether the subscription is activated |
| activated_at | timestamp | Activation timestamp |
| is_trial_converted | bool | Whether the trial converted to paid |
| trial_converted_at | timestamp | Trial conversion timestamp |
| user_subscription_number | int | Which subscription number this is for the user |
| first_platform | string | Platform of first visit (web, app, …) |
| first_channel_grouping | string | Marketing channel |
| user_state | string | US state of the user |
| _updated_ts | timestamp | Last update timestamp |

---

## subscription_terms

One row per billing term (renewal period) within a subscription.

| Column | Type | Description |
|---|---|---|
| subscription_term_id | string | Primary key |
| subscription_id | string | FK → subscriptions |
| common_id | string | Cross-table join key |
| term_number | int | 1 = first term, 2 = first renewal, … |
| term_started_at | timestamp | Term start |
| term_ended_at | timestamp | Timestamp of subscription termination. NULL if the term is still active |
| term_active_until | timestamp | Timestamp of the date that the term is active until |
| next_term_started_at | timestamp | Start of the next renewal |
| term_status | string | `active`, `canceled`, `paused`, … |
| termination_type | string | How the term ended |
| is_new_start | bool | True for the first term of the subscription |
| is_paid | bool | Whether this term was paid |
| is_delinquent | bool | The latest charge on the latest term invoice failed, but the subscription was still left active after that failed charge |
| is_failed_payment_canceled | bool | The latest charge on the latest term invoice failed and that failure caused the subscription to be paused or canceled |
| cancel_requested_at | timestamp | When cancellation was requested |
| cancel_reason | string | Cancellation reason |
| _updated_ts | timestamp | Last update timestamp |

---

## subscription_plan_terms

Drug/plan details within each subscription term. A term can have multiple plan terms if the user switched plans mid-term.

| Column | Type | Description |
|---|---|---|
| subscription_plan_term_id | string | Primary key |
| subscription_term_id | string | FK → subscription_terms |
| subscription_id | string | FK → subscriptions |
| plan_term_number | int | Sequential plan term within the subscription |
| plan_term_started_at | timestamp | When this plan started |
| plan_term_ended_at | timestamp | When this plan ended (null if current) |
| plan_term_status | string | `active`, `ended` |
| is_latest_plan_term | bool | True if this is the current plan |
| plan_id | string | Plan identifier |
| previous_plan_id | string | Plan before a switch |
| plan_name | string | Human-readable plan name (drug, dose, quantity, term length) |
| plan_variant | string | Variant code |
| term_months | int | Length of the plan term in months |
| drug_id | int | Drug identifier |
| drug_name | string | e.g. "sildenafil", "tadalafil (cialis)" |
| drug_strength | string | e.g. "2.5mg", "20mg" |
| regimen | string | DAILY or AS_NEEDED |
| monthly_dose | int | Doses per month |
| starting_fulfillment_type | string | Fulfillment type at term start |

---

## subscription_charges

One row per payment charge attempt.

| Column | Type | Description |
|---|---|---|
| charge_id | string | Primary key |
| invoice_id | string | FK → subscription_invoices |
| subscription_id | string | FK → subscriptions |
| event_name | string | e.g. `subscription_payment_succeeded`, `subscription_payment_failed` |
| occurred_at | timestamp | When the charge was attempted |
| attempt_number | int | Retry attempt number (1 = first try) |
| is_latest_charge | bool | Whether this is the latest charge for the invoice |
| amount_due | decimal | Amount attempted |
| gross_revenue | decimal | Gross revenue collected |
| net_revenue | decimal | Net revenue after refunds |
| is_paid | bool | Charge succeeded |
| is_failed | bool | Charge failed |
| was_paid | bool | Historical flag |
| is_refunded | bool | Whether refunded |
| refunded_at | timestamp | Refund timestamp |
| failure_reason | string | Failure reason if failed |
| sub_state_after_charge | string | Subscription state immediately after this charge |
| card_brand | string | e.g. mastercard, visa |
| wallet_type | string | e.g. apple_pay |

---

## subscription_invoices

One row per invoice (one per billing cycle per subscription term).

| Column | Type | Description |
|---|---|---|
| invoice_id | string | Primary key |
| subscription_id | string | FK → subscriptions |
| subscription_term_id | string | FK → subscription_terms |
| common_id | string | Cross-table join key |
| invoice_number | int | Sequential invoice number within the subscription |
| term_number | int | Which term this invoice belongs to |
| invoice_status | string | `paid`, `failed`, `refunded`, … |
| is_paid | bool | |
| is_failed | bool | |
| is_refunded | bool | |
| caused_cancellation | bool | Whether this failed invoice triggered cancellation |
| is_delinquent | bool | |
| is_trial_invoice | bool | |
| is_trial_conversion_invoice | bool | First paid invoice after trial |
| amount_due | decimal | |
| gross_revenue | decimal | |
| net_revenue | decimal | |
| num_charges | int | Number of charge attempts for this invoice |
| paid_at | timestamp | |
| failed_at | timestamp | |
| first_charge_at | timestamp | |
| latest_charge_at | timestamp | |
| term_started_at / term_ended_at | timestamp | Term bounds for this invoice |
| drug_id / drug_name | int / string | Drug on this invoice |
| next_invoice_number | int | Expected next invoice number |
| expected_refill_dt | date | Expected next refill date |
| is_dup | bool | Duplicate invoice flag |

---

## subscriptions_churn

Pre-aggregated churn view. One row per subscription term with payment summary and a forecasted churn date.  
**This is the primary input for the churn label in Phase 3.**

| Column | Type | Description |
|---|---|---|
| common_id | string | Cross-table join key |
| subscription_id | string | FK → subscriptions |
| subscription_term_id | string | FK → subscription_terms |
| term_number | int | 1 = first term |
| is_reactivated_term | bool | True if the user re-subscribed after canceling |
| subscription_category | string | "conditions" |
| subscription_name | string | "conditions: erectile dysfunction" |
| is_activated | bool | |
| term_started_at | timestamp | |
| term_ended_at | timestamp | Null if still active |
| first_invoice_paid_dt | date | Date of first paid invoice in this term |
| last_invoice_paid_dt | date | Date of last paid invoice in this term |
| count_paid_invoices | int | Number of paid invoices in this term |
| total_paid_amount | decimal | Total gross amount paid |
| net_paid_amount | decimal | Net amount after refunds |
| plan_name | string | Plan at term start |
| term_months | int | Term length in months |
| cancel_requested_at | timestamp | When cancellation was requested |
| forecasted_churn_date | timestamp | Model-forecasted churn date |
| user_state | string | US state |

---

## subscription_plan_types

Reference table for plan definitions — one row per plan. Maps `plan_id` to drug, dosing, pricing, and term details.

| Column | Type | Description |
|---|---|---|
| plan_id | string | Primary key |
| subscription_type | string | e.g. `CONDITION` |
| tenant_id | string | e.g. `gdrx` |
| display_name | string | Human-readable plan name |
| tags | string | JSON array of tags (e.g. `BEST_VALUE`) |
| created_at | timestamp | When the plan was created |
| updated_at | timestamp | Last update timestamp |
| condition_id | int | 135 = erectile dysfunction |
| condition_name | string | e.g. `Erectile Dysfunction` |
| drug_id | int | Drug identifier |
| drug_name | string | e.g. `tadalafil (cialis)`, `sildenafil` |
| drug_strength | string | e.g. `20mg`, `2.5mg` |
| regimen | string | `AS_NEEDED` or `DAILY` |
| monthly_dose | int | Doses per month |
| variant | string | Plan variant code |
| term_months | int | Term length in months |
| term_price | decimal | Price for the full term |
| billing_price | decimal | Billed amount |
| prescription_price | decimal | Prescription price |
| quantity_per_fill | int | Quantity dispensed per fill |
| desired_refill_count | int | Expected number of refills |
| billing_duration_count | int | Number of billing periods |
| days_supply_per_fill | int | Days supply per fill |
| fulfillment_method | string | e.g. `delivery` |
| default_store_id | string | Default pharmacy/store |
| stripe_price_id | string | Stripe price identifier |
| plan_generation_id | string | Plan generation group |
| display_image_uri | string | Image URL for the plan |
| _fivetran_deleted | boolean | Soft-delete flag from Fivetran sync |
| _fivetran_synced | timestamp | Last Fivetran sync timestamp |
