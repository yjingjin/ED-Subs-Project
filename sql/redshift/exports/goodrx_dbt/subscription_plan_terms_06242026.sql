-- query datetime: 06/24/2026 4:40 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_plan_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
