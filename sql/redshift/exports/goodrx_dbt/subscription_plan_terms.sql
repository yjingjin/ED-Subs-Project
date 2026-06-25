-- goodrx_dbt.subscription_plan_terms
-- Plan details within each subscription term (drug, dosage, regimen, term length).
-- ED-filtered via condition_id = 135.
-- Export from DataGrip → subscription_plan_terms.csv → upload to Databricks volume → ed_bronze.subscription_plan_terms

SELECT
    subscription_id,
    common_id,
    subscription_term_id,
    subscription_plan_term_id,
    term_number,
    plan_term_number,
    term_started_at,
    term_ended_at,
    term_status,
    plan_term_started_at,
    plan_term_ended_at,
    plan_term_status,
    next_plan_term_started_at,
    next_plan_term_plan_id,
    is_latest_plan_term,
    subscription_category,
    subscription_subcategory,
    subscription_name,
    condition_name,
    condition_id,
    plan_id,
    previous_plan_id,
    plan_name,
    plan_variant,
    term_months,
    drug_id,
    drug_name,
    drug_strength,
    regimen,
    monthly_dose,
    starting_visit_id,
    starting_fulfillment_type
FROM goodrx_dbt.subscription_plan_terms
WHERE condition_id = 135  -- erectile dysfunction
;
