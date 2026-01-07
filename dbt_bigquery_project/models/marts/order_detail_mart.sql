select
    date(timestamp(processed_at)) as order_date,
    order_number,
    total_price as order_total,
    customer_id,
    shipping_address_first_name,
    shipping_address_address1,
    shipping_address_phone,
    shipping_address_city,
    shipping_address_zip,
    shipping_address_province,
    shipping_address_country,
    shipping_address_last_name,
    shipping_address_address2,
    shipping_address_company,
    shipping_address_latitude,
    shipping_address_longitude,
    shipping_address_name,
    shipping_address_country_code,
    shipping_address_province_code

from {{ ref('orders_fact_staging') }} as o

--join in the line detail; let's do a order/line detail dataset, product name and ID, customer location, line and order level totals