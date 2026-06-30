-- query datetime: 06/30/2026 5:08 pm EST
SELECT
    *
FROM goodrx_dbt.int_subs_kafka__events
WHERE
    LOWER(condition_name) = 'erectile dysfunction'

