-- query datetime: 07/02/2026 6:01 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_invoices
WHERE
    LOWER(subscription_subcategory) = 'erectile dysfunction'




