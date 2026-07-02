# ED Subs Key Definitions

## Business

**Active subscriber:** a user with a successful subscription payment and an Rx written.

> Example: if a user is successfully charged and a provider writes the Rx, the user is active.

**Period:** one paid billing cycle. A period can be 1, 3, or 6 months long.

> Example: if a user starts an ED subscription on 7/1 and ends on 12/31 on a 3-month cadence, the term includes two periods: 7/1–9/30 and 10/1–12/31.

**Subscription term:** the continuous paid period of access. A term can include one or more consecutive periods.

> Example: if a user starts a 3-month ED subscription on 7/1 and remains subscribed until 12/31, then 7/1–12/31 is one continuous subscription term, made up of two 3-month periods.

**Cancellation request:** the subscriber asks to cancel the subscription. The subscription remains active through the already-paid term.

> Example: if a user cancels on April 10 but the current term ends on May 31, the subscription stays active until June 1.

**Undo cancellation:** a subscriber reverses a prior cancellation request before it takes effect.

> Example: if a user submits a cancellation request on April 10 and reverses it on April 20 before the term ends, that is an undo cancellation.

---

## Modeling (Milestone 1)

**Snapshot date:** 2026-05-01. The date used to define the cohort and freeze observable features.

**Label window:** 2026-05-01 (exclusive) to 2026-05-31 (inclusive) (30 days after snapshot).

**Label end date:** 2026-05-31.

**Cohort:** all active subscriptions with no prior cancellation request as of the snapshot date.

> Table: `ed_silver_subscription_terms_qualified`

```
term_started_at <= '2026-05-01'
AND (term_ended_at IS NULL OR term_ended_at > '2026-05-01')
AND (cancel_requested_at IS NULL OR cancel_requested_at > '2026-05-01')
```

> Subscriptions that already had a cancellation request on or before 2026-05-01 are excluded — they are already-decided churners and cannot be prevented. Since reactivation was not available before June 2026, there are limited reactivation data, so reactivation is not in the scope of Milstoen 1. In this way, these are definite churners.

**Label (Milestone 1):** did the subscriber request cancellation within the 30-day label window?

```
is_cancelled = 1  if cancel_requested_at BETWEEN '2026-05-02' AND '2026-05-31'
is_cancelled = 0  if cancel_requested_at IS NULL OR cancel_requested_at > '2026-05-31'
```

> Subscriptions are auto-renew. A subscriber must actively cancel to stop being charged.
> Therefore, voluntary churn = cancellation request. The label directly captures user intent.

**Voluntary churn:** churn driven by an explicit user cancellation request (`cancel_requested_at IS NOT NULL`).

**Involuntary churn:** churn driven by payment failure (ask Kevin:`is_failed_payment_canceled = TRUE` at the term level). Involuntary churn is included in EDA but excluded from model training — it is not driven by user intent and is not actionable by a prevention model.

**Reactivation:** not in scope for Milestone 1. Reactivation only became available after June 2026 and data is sparse. Churn is treated as irreversible for this milestone.

**Deferred renewal:** a subscriber pushes their next renewal date to a later date
(`event_name = 'term_renewal_time_changed' AND changed_by = 'CHANGED_BY_USER'` in `int_subs_kafka__events`).
Deferred subscribers remain active (term not ended) and naturally remain in the cohort with `is_cancelled = 0`.

**Production use:** the model scores all active subscriptions at any point in time and predicts:

> "Will this subscriber request cancellation in the next 30 days?"

This is actionable — we can intervene before the cancellation request is submitted.

---

## Implementation notes

- `Ask kevin: term_ended_at` is only populated when a subscription is already cancelled/terminated. For active subscriptions, use `term_active_until` for the expected end date.

