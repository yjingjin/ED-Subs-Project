-- query datetime: 06/24/2026 1:30 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
    AND is_paid IS TRUE



