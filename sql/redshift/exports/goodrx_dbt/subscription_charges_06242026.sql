-- query datetime: 06/24/2026 5:30 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_charges
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
