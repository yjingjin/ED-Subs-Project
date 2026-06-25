-- query datetime: 06/22/2026 6 pm EST
SELECT
    *
FROM goodrx_dbt.subscriptions
WHERE
    condition_name = 'erectile dysfunction'
    AND is_paid IS TRUE
    AND is_activated IS TRUE