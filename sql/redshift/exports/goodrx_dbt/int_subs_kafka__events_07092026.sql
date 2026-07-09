-- query datetime: 07/09/2026 5:20 pm
-- Kafka events for ED subscriptions. Three event types:
--   term_renewal_time_changed  → user-initiated deferral
--   upcoming_term_renewal_notified → renewal notification
--   condition_subscription_prescription_written → Rx written (proxy for intake/visit completion)
SELECT
    *
FROM goodrx_dbt.int_subs_kafka__events
WHERE
    LOWER(condition_name) = 'erectile dysfunction'
    AND event_name IN (
        'upcoming_term_renewal_notified',
        'term_renewal_time_changed',
        'condition_subscription_prescription_written',
        'cancellation_intent_declared',
        'condition_subscription_new_prescription_required',
        'plan_change_requested',
        'clinician_plan_approved',
        'order_fulfillment_confirmed',
        'canceled',
        'cancellation_intent_revoked'
    )
