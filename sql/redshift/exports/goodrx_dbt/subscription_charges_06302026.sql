-- query datetime: 06/30/2026 5:13 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_charges
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
