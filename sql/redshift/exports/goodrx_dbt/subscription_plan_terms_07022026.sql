-- query datetime: 07/02/2026 6:02 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_plan_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
