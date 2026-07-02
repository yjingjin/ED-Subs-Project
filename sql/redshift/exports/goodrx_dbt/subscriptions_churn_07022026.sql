-- query datetime: 07/02/2026 6:05 pm EST
SELECT
    *
FROM goodrx_dbt.subscriptions_churn
WHERE
    LOWER(subscription_name) = 'conditions: erectile dysfunction'



