WITH raw_orders AS (

    select * from (

    SELECT
        id as order_id,
        REPLACE(
            REPLACE(
                REPLACE(line_items, "'", '"'),
                "None", "null"
            ),
            "True", "true"
            ) as items_json,
        row_number() over (partition by id order by updated_at desc) as rn

    FROM {{ source('raw_data','orders_raw') }}
    ) 
    where rn = 1
)

SELECT
    JSON_VALUE(items, '$.id') AS line_item_id,
    order_id,
    JSON_VALUE(items, '$.admin_graphql_api_id') AS admin_graphql_api_id,
    JSON_VALUE(items, '$.attributed_staffs') AS attributed_staffs,
    JSON_VALUE(items, '$.current_quantity') AS current_quantity,
    JSON_VALUE(items, '$.fulfillable_quantity') AS fulfillable_quantity,
    JSON_VALUE(items, '$.fulfillment_service') AS fulfillment_service,
    JSON_VALUE(items, '$.fulfillment_status') AS fulfillment_status,
    JSON_VALUE(items, '$.grams') AS grams,
    JSON_VALUE(items, '$.name') AS `name`,
    JSON_VALUE(items, '$.price') AS price,
    JSON_VALUE(items, '$.price_set') AS price_set,
    JSON_VALUE(items, '$.product_exists') AS product_exists,
    JSON_VALUE(items, '$.properties') AS properties,
    JSON_VALUE(items, '$.quantity') AS quantity,
    JSON_VALUE(items, '$.requires_shipping') AS requires_shipping,
    JSON_VALUE(items, '$.sku') AS sku,
    JSON_VALUE(items, '$.taxable') AS taxable,
    JSON_VALUE(items, '$.title') AS title,
    JSON_VALUE(items, '$.total_discount') AS total_discount,
    JSON_VALUE(items, '$.total_discount_set') AS total_discount_set,
    JSON_VALUE(items, '$.variant_id') AS variant_id,
    JSON_VALUE(items, '$.variant_inventory_management') AS variant_inventory_management,
    JSON_VALUE(items, '$.variant_title') AS variant_title,
    JSON_VALUE(items, '$.vendor') AS vendor,
    JSON_VALUE(items, '$.tax_lines') AS tax_lines,
    JSON_VALUE(items, '$.duties') AS duties,
    JSON_VALUE(items, '$.discount_allocations') AS discount_allocations,

FROM raw_orders,
UNNEST(JSON_EXTRACT_ARRAY(items_json)) AS items

