-- query datetime: 07/02/2026 6:21 pm EST
-- Filters: ED only (condition_name = 'erectile dysfunction')

SELECT
    *
FROM goodrx_dbt.subscriptions
WHERE
    condition_name = 'erectile dysfunction'


