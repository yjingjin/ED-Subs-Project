-- query datetime: 06/24/2026 3 pm EST
SELECT
    *
FROM goodrx_dbt.subscription_invoices
WHERE
    LOWER(subscription_subcategory) = 'erectile dysfunction'




