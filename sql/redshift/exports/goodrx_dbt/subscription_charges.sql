-- goodrx_dbt.subscription_charges
-- Individual payment charge events per subscription. ED-filtered via condition_id = 135.
-- Export from DataGrip → subscription_charges.csv → upload to Databricks volume → ed_bronze.subscription_charges

SELECT
    charge_id,
    event_name,
    invoice_id,
    subscription_id,
    is_latest_charge,
    attempt_number,
    occurred_at,
    common_id,
    activation_count,
    order_id,
    visit_id,
    prescription_id,
    plan_id,
    plan_name,
    subscription_category,
    subscription_name,
    condition_name,
    condition_id,
    drug_id,
    drug_name,
    failure_reason,
    amount_due,
    payment_method_id,
    current_term_end_at,
    sub_state_after_charge,
    card_brand,
    wallet_type,
    is_refunded,
    refunded_at,
    is_failed,
    was_paid,
    is_paid,
    status,
    gross_revenue,
    net_revenue
FROM goodrx_dbt.subscription_charges
WHERE condition_id = 135  -- erectile dysfunction
;
