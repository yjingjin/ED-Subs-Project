-- query datetime: 06/24/2026 6 pm EST
SELECT
    *
FROM goodrx_dbt.int_subs_kafka__events
WHERE
    LOWER(condition_name) = 'erectile dysfunction'

