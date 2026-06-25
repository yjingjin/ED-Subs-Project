-- goodrx_dbt.subscription_terms
-- One row per subscription term (billing period). ED-filtered via condition_id = 135.
-- Export from DataGrip → subscription_terms.csv → upload to Databricks volume → ed_bronze.subscription_terms

SELECT
    common_id,
    subscription_id,
    subscription_term_id,
    term_number,
    subscription_category,
    subscription_subcategory,
    subscription_name,
    condition_name,
    condition_id,
    _sub_activated_at,
    term_started_at,
    next_term_started_at,
    term_ended_at,
    term_status,
    _status,
    termination_type,
    term_active_until,
    is_new_start,
    had_trial,
    is_trial_converted,
    trial_converted_at,
    trial_is_ended,
    trial_ended_at,
    trial_conversion_expected_at,
    is_trial_canceled,
    is_manual_trial_canceled,
    is_failed_payment_trial_canceled,
    first_plan_id,
    latest_plan_id,
    first_active_visit_id,
    cancel_requested_at,
    cancel_expected_at,
    cancel_reason,
    is_paid,
    paid_at,
    is_delinquent,
    is_failed_payment_canceled,
    trial_is_delinquent,
    refunded_at,
    is_refunded,
    _updated_ts
FROM goodrx_dbt.subscription_terms
WHERE condition_id = 135  -- erectile dysfunction
;
