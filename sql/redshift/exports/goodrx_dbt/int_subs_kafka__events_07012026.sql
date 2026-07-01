-- query datetime: 07/01/2026 12:02 pm
-- Kafka events for ED subscriptions: deferral (term_renewal_time_changed) and
-- renewal notification (upcoming_term_renewal_notified) events.

SELECT
    *
FROM goodrx_dbt.int_subs_kafka__events
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
    AND event_name IN ('upcoming_term_renewal_notified', 'term_renewal_time_changed')
