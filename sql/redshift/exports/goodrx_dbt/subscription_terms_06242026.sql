-- query datetime: 06/29/2026 3 pm EST
select
    *
FROM goodrx_dbt.subscription_terms
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
    AND is_paid IS TRUE

-- with ever_paused_subs as(
-- SELECT
--     subscription_id,
--     subscription_term_id,
--     term_started_at,
--     term_ended_at,
--     term_status
-- FROM goodrx_dbt.subscription_terms
-- WHERE
--     LOWER(condition_name) = 'erectile dysfunction'
--     AND is_paid IS TRUE
--     AND subscription_id in (
--         select
--             subscription_id
--         from goodrx_dbt.subscription_terms where term_status = 'paused'
--     )
-- order by subscription_id, term_started_at)

-- select
--     subscription_id,
--     count(subscription_term_id) as n_terms
-- from ever_paused_subs
-- group by 1
-- having n_terms > 1



-- select
--     term_status,
--     count(*) as cnt
-- FROM goodrx_dbt.subscription_terms
-- WHERE
--     LOWER(condition_name) = 'erectile dysfunction'
--     AND is_paid IS TRUE
-- group by 1

