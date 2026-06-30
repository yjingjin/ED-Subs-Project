-- query datetime: 06/30/2026 5:16 pm EST
select
    *
FROM goodrx_dbt.subscription_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
    AND is_paid IS TRUE