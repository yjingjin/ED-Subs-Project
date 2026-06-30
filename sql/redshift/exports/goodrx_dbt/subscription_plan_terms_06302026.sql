-- query datetime: 06/30/2026 5:15 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_plan_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
