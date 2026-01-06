select
    date(timestamp(processed_at)) as order_date,
    order_number,
    total_price as order_total

from {{ ref('orders_fact_staging') }}
