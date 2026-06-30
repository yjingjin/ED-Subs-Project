-- query datetime: 06/30/2026 5:17 pm EST
SELECT
    *
FROM goodrx_dbt.subscriptions_churn
WHERE
    LOWER(subscription_name) = 'conditions: erectile dysfunction'



