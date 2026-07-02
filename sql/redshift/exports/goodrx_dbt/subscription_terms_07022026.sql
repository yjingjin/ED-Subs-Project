-- query datetime: 07/02/2026 6:09 pm EST
select
    *
FROM goodrx_dbt.subscription_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'