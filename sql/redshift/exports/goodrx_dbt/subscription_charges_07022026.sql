-- query datetime: 07/02/2026 6:13 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_charges
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
