# ED Subs Key Definitions

## Business

**Active subscriber:** a user with an Rx written and a successful subscription payment.

> Example: if a provider writes the Rx and a user is successfully charged, the user is active.

**Period:** one paid billing cycle. A period can be 1, 3, or 6 months long.

> Example: if a user starts an ED subscription on 7/1 and ends on 12/31 on a 3-month cadence, the term includes two periods: 7/1–9/30 and 10/1–12/31.

**Subscription term:** the continuous paid period of access. A term can include one or more consecutive periods.

> Example: if a user starts a 3-month ED subscription on 7/1 and remains subscribed until 12/31, then 7/1–12/31 is one continuous subscription term, made up of two 3-month periods.

**Cancellation request:** the subscriber asks to cancel the subscription. The subscription remains active through the already-paid term.

> Example: if a user cancels on April 10 but the current term ends on May 31, the subscription stays active until June 1.

**Undo cancellation:** a subscriber reverses a prior cancellation request before it takes effect. Available starting from May 2026.

> Example: if a user submits a cancellation request on June 10 and reverses it on June 20 before the term ends, that is an undo cancellation.

---

## Modeling (Milestone 1)

**Prediction point:** the moment at which features are frozen and the model asks "will this subscriber cancel in the next 30 days?" Each subscriber generates multiple prediction points depending on the rolling window approach used (see below).

**Label window:** 30 days starting from each prediction point (inclusive on both ends).

**Label end date:** `prediction_point + 29 days` per observation. The outer constraint is that this must fall within the data pull date (2026-06-30), so the latest valid prediction point is 2026-06-01.

![Cohort & Label Logic](cohort_label_logic.png)

**Cohort:** all active, non-reactivated subscription terms. No fixed snapshot date — the prediction point varies per subscriber and per observation window.

> Table: `ed_silver_subscription_terms_qualified` (base pool; date filtering applied per prediction point in rolling window logic)

```
-- Base cohort filter (applied once)
term_started_at < '2026-06-01'           -- must have started before data cutoff
AND NOT (                                 -- exclude reactivated terms
    term_number > 1
    AND is_new_start = FALSE
    AND EXISTS (
        SELECT 1 FROM subscription_terms prev
        WHERE prev.subscription_id = t.subscription_id
          AND prev.term_number = t.term_number - 1
          AND prev.term_ended_at IS NOT NULL
    )
)
```

**Reactivated terms:** (a subscriber churned and started a new term under the same `subscription_id`) are excluded. They have different behavior from continuously renewing subscribers and will be modeled separately in future milestones.

**Label:** did the subscriber request cancellation within 30 days after the prediction point?

```
is_cancelled = 1  if cancel_requested_at BETWEEN prediction_point
                                              AND DATEADD(DAY, 29, prediction_point)
is_cancelled = 0  otherwise
```

> SQL BETWEEN is inclusive on both ends, so +29 days gives exactly 30 days (day 0 through day 29).

**Label (Milestone 1):** did the subscriber request cancellation within 30 days of the prediction point?

```
is_cancelled = 1  if cancel_requested_at BETWEEN prediction_point
                                              AND DATEADD(DAY, 29, prediction_point)
is_cancelled = 0  otherwise
```

> Subscriptions are auto-renew. A subscriber must actively cancel to stop being charged.
> Therefore, voluntary churn = cancellation request. The label directly captures user intent.

**Voluntary churn:** churn driven by an explicit user cancellation request (`cancel_requested_at IS NOT NULL`).

**Involuntary churn:** churn driven by payment failure (ask Kevin:`is_failed_payment_canceled = TRUE` at the term level). Involuntary churn is included in EDA but excluded from model training — it is not driven by user intent and is not actionable by a prevention model.

**Reactivation:** A new term_id is created under the same susbcirption_id. not in scope for Milestone 1. Reactivation only became available after June 2026 and data is sparse. Churn is treated as irreversible for this milestone.

**Deferred renewal:** a subscriber pushes their next renewal date to a later date
(`event_name = 'term_renewal_time_changed' AND changed_by = 'CHANGED_BY_USER'` in `int_subs_kafka__events`).

**Production use:** the model scores all active subscriptions at any point in time and predicts:

> "Will this subscriber request cancellation in the next 30 days?"

This is actionable — we can intervene before the cancellation request is submitted.

---

## Rolling window modeling approaches (Milestone 1)

Two rolling window approaches will be tested:

### Approach A — Forward rolling window

Starting from each subscription's `term_started_at`, roll forward in 30-day steps until June 2026. Each step creates one training observation:

```
Observation 1: snapshot = term_started_at,          label window = [+0, +30 days]
Observation 2: snapshot = term_started_at + 30,     label window = [+30, +60 days]
Observation 3: snapshot = term_started_at + 60,     label window = [+60, +90 days]
...until snapshot >= 2026-06-01 or term ends
```

- **Label**: did `cancel_requested_at` fall within the label window? → `is_cancelled` 0/1
- **Features**: all features computed as of the prediction point (no future data)
- Applies to both churners and non-churners symmetrically

### Approach B — Backward from cancellation

For churned subscribers only, starting from `cancel_requested_at`, roll backward in 30-day steps until `term_started_at`:

```
Observation 1 (label=1): snapshot = cancel_requested_at - 30,   label window = [snapshot, +30 days]
Observation 2 (label=0): snapshot = cancel_requested_at - 60,   label window = [snapshot, +30 days]
Observation 3 (label=0): snapshot = cancel_requested_at - 90,   label window = [snapshot, +30 days]
...until snapshot <= term_started_at
```

- **For non-churners**: apply the same backward stepping from `term_active_until` or data pull date
- **Features**: all features computed as of each prediction point
- Ensures every churn event is captured with exactly one label=1 row at 30 days before cancellation

### Comparison

| | Forward | Backward |
|---|---|---|
| Anchor point | term_started_at | cancel_requested_at (churners) |
| Non-churner anchor | Same (term_started_at) | Needs separate definition |
| Mirrors production scoring | Yes | Partially |
| Implementation | Straightforward | More complex for non-churners |

Both approaches will be implemented and evaluated. The better-performing one will be used for the final model.

---

## Implementation notes

- `Ask kevin: term_ended_at` is only populated when a subscription is already cancelled/terminated. For active subscriptions, use `term_active_until` for the expected end date.

