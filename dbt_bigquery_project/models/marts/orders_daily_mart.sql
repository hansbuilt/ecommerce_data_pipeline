select
    date(timestamp(processed_at)) as order_date,
    count(distinct id) as order_count,
    sum(total_price) as revenue
from {{ ref('orders_fact_staging') }}
group by processed_at
order by processed_at