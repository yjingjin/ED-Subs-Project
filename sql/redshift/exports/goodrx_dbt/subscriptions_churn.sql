-- goodrx_dbt.subscriptions_churn
-- Pre-built churn view: one row per subscription term with payment summary and forecasted churn date.
-- ED-filtered via subscription_name.
-- Export from DataGrip → subscriptions_churn.csv → upload to Databricks volume → ed_bronze.subscriptions_churn

SELECT
    common_id,
    subscription_id,
    subscription_term_id,
    term_number,
    is_reactivated_term,
    subscription_category,
    subscription_name,
    is_activated,
    term_started_at,
    term_ended_at,
    first_invoice_paid_dt,
    last_invoice_paid_dt,
    count_paid_invoices,
    total_paid_amount,
    net_paid_amount,
    plan_name,
    term_months,
    cancel_requested_at,
    forecasted_churn_date,
    user_state
FROM goodrx_dbt.subscriptions_churn
WHERE subscription_name LIKE '%erectile dysfunction%'
;
