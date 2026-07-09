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

**Qualified subscription terms**: 

- The **first terms** of subscriptions
    - Why exclude reactivated terms?
    
        Reactivation is recent product change and the **sample size is limited**. Including   them    - would mix different product states and make the sample less clean.

- were **activated**, and

- **started before June 1, 2026**

    - Why require terms to start before June 1, 2026?

        This ensures every included term has a full 30-day observation window. It also ensures the  churn outcome is fully observable as of today.

**Preidction target**: cancellation in the next 30 days

- Why cancellation instead of effective churn?

    This analysis focuses on pre-churn behavior in the pre-reactivation product era, when   **cancellation almost certainly led to churn**. Therefore, the prediction target should be cancellation so that **action can be taken in time**.

**Prediction point (T): Features are frozen at T (all data up to and including T is used).

**Label window:** T+1, T+30 — the 30 days **after** T, excluding T itself.

> Same-day cancellations (`cancel_requested_at = term_started_at::date`) are tracked separately as `cancelled_at_start` in the label table and excluded from model training (they are already-decided before the model could act).

![Labeling Logic](cohort_label_logic.png)

**Voluntary churn:** churn driven by an explicit user cancellation request (`cancel_requested_at IS NOT NULL`).

**Involuntary churn:** churn driven by payment failure (`is_failed_payment_canceled = TRUE` at the term level). Included in EDA but excluded from model training — not driven by user intent and not actionable by a prevention model.

**Reactivation:** a new `subscription_term_id` created under the same `subscription_id` after a churn. Not in scope for Milestone 1 — reactivation only became available after June 2026 and data is sparse. Churn is treated as irreversible for this milestone.

**Deferred renewal:** a subscriber pushes their next renewal date to a later date (`event_name = 'term_renewal_time_changed' AND changed_by = 'CHANGED_BY_USER'` in `int_subs_kafka__events`).

**Production use:** the model scores all active subscriptions at any point in time and predicts:

> "Will this subscriber request cancellation in the next 30 days?"

This is actionable — we can intervene before the cancellation request is submitted.

---

## Rolling window modeling approaches (Milestone 1)

**Approach: Forward rolling window with weekly snapshots.**

For each subscriber, generate one observation per week starting from `term_started_at`:

```
snapshot_0 = term_started_at
snapshot_1 = term_started_at + 7 days
snapshot_2 = term_started_at + 14 days
...
```

**Stop rules:**
- Churners: stop when `snapshot >= cancel_requested_at`
- Non-churners: stop when `snapshot > 2026-05-31`

**Latest valid snapshot:** 2026-05-31 — ensures every observation has a fully observable 30-day label window within the label cutoff date (2026-06-30). Note: data was pulled on 2026-07-02; the label cutoff date of 2026-06-30 is a deliberate design choice, not constrained by the pull date.

**Label per snapshot:**
```
label = 1  if cancel_requested_at BETWEEN snapshot + 1 AND snapshot + 30
label = 0  otherwise
```

"Do not stop at first 1" — once the label becomes 1, continue generating weekly snapshots. A churner may contribute multiple consecutive label=1 rows as the cancel date moves through successive 30-day windows.

**Features per snapshot:** all data observable strictly up to `snapshot` date (no future leakage). Static features (drug, cadence, regimen) are constant across snapshots. Dynamic features (invoice history, order counts, events) must be computed as of each snapshot date.

**Class imbalance:** will be addressed during model training.

---

## Implementation notes

- `Ask kevin: term_ended_at` is only populated when a subscription is already cancelled/terminated. For active subscriptions, use `term_active_until` for the expected end date.