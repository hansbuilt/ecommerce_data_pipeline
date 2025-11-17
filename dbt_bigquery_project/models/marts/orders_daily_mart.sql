select
    created_at as order_date,
    count(distinct id) as order_count,
    sum(total_price) as revenue
from {{ ref('orders_fact_staging') }}
group by created_at
order by created_at