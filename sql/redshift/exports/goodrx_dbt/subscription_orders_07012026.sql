-- query datetime: 07/01/2026 12:28 pm EST
select
    orders.*
from goodrx_dbt.subscription_orders as orders
join goodrx_dbt.subscription_invoices as invs
on invs.latest_order_id = orders.order_id
where  LOWER(invs.subscription_subcategory) = 'erectile dysfunction'