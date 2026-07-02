-- query datetime: 07/02/2026 5:56 pm EST
select
    orders.*
from goodrx_dbt.subscription_orders as orders
join goodrx_dbt.subscription_invoices as invs
on invs.latest_order_id = orders.order_id
where  LOWER(invs.subscription_subcategory) = 'erectile dysfunction'