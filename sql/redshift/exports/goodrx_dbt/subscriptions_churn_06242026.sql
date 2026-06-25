-- query datetime: 06/24/2026 6 pm EST
SELECT
    *
FROM goodrx_dbt.subscriptions_churn
WHERE
    LOWER(subscription_name) = 'conditions: erectile dysfunction'


