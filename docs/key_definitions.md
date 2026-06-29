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

## Modeling

**Snapshot date:** the upper-bound date used to define the modeling cohort and select each user's current period. In this framework, the snapshot date is **2026-05-15**.
> Example: when building the cohort for the 2026-05-15 snapshot, only information available on or before 2026-05-15 is used.

**Current period:** the most recent subscription period ending on or before the snapshot date.
> Example: if a user had periods ending on 2025-12-15, 2026-02-15, and 2026-05-10, then for the 2026-05-15 snapshot, the current period is the period ending on 2026-05-10.

**Churn:** a subscriber is considered churned if they do not renew at the end of a subscription period. For Milestone 1, operationally: no new valid service subscription within 30 days after the current period expires.
> Example: if a user's current period ends on 2026-05-15 and they do not start a new valid service subscription by 2026-06-14, they are labeled as churned.

**Retained:** the subscriber remains subscribed either by continuing into the next period through renewal or by starting a new valid service subscription within 30 days after the current period ends.
> Example: if a user's period ends on 2026-05-15 and they renew immediately, they are retained. If they start a new valid service subscription on 2026-06-01, they are also retained.

**Label end date:** the last date used to label whether the current period ends in churn or retention. This ensures enough time has passed to observe the outcome for every user in the cohort. In this framework, the label end date is **2026-06-14**, so each period selected at the 2026-05-15 snapshot has a full 30-day outcome window.
> Example: if a user's current period ends on 2026-05-15, we observe through 2026-06-14 to determine whether they are retained or churned.

**Prediction point:** the moment when we "freeze" what we know about a user and ask: based on everything up to this point, will this user churn in the future? It is also the moment when we ask the model for a churn risk score. In this framework, the prediction point is **50% through the current period**.
> Example: for a 6-month period, the prediction point is at month 3. For a 1-month period, it is halfway through the month.
> Exclude users who submitted a cancellation request before the prediction point. The model is then scoped to users who have not yet decided at the prediction point, which matches the prevention use case. Those "already decided" users should not go to a prevention model.

**Cohort:** ED users who had at least one subscription period ending on or before the snapshot date, but no earlier than 12 months before the snapshot date.
> Example: for a 2026-05-15 snapshot, eligible users must have at least one period ending between 2025-05-15 and 2026-05-15.
> As of Milestone 1 (June 2026), the 12-month restriction does not affect cohort selection. However, it will become important as the product matures, because behavior, product, and market conditions from the earliest cohorts may no longer be representative of current users.

**Voluntary churn:** churn driven by an explicit user decision to stop the subscription, such as a user-initiated cancellation.
> Example: if a user cancels and does not reverse or start a new valid subscription within 30 days after term ends, that is voluntary churn.

**Involuntary churn:** churn driven by payment, operational, or administrative failure rather than clear user intent, such as charge failure.
> Example: if a renewal attempt fails because the payment method is declined and the user does not re-subscribe within 30 days after term ends, that is involuntary churn.

**Reactivation:** a subscriber starts a new active term after the previous term ends without a renewal. Reactivation does not necessarily imply prior churn, because it can occur within the 30-day churn window.
> Example: if a user's term ends on 2026-05-15 without a renewal and they start a new active term on 2026-05-25, that is a reactivation, but the user is still considered retained because it happened within the 30-day window.
