WITH raw_products AS (
    SELECT
        id as product_id,
        REPLACE(
            REPLACE(
                REPLACE(images, "'", '"'),
                "None", "null"
            ),
            "True", "true"
            ) as images_json
    FROM {{ source('raw_data','products_raw') }}
)

SELECT
    JSON_VALUE(imgs, '$.id') AS product_image_id,
    product_id,
    JSON_VALUE(imgs, '$.alt') AS alt,
    JSON_VALUE(imgs, '$.position') AS position,
    JSON_VALUE(imgs, '$.created_at') AS created_at,
    JSON_VALUE(imgs, '$.updated_at') AS updated_at,
    JSON_VALUE(imgs, '$.admin_graphql_api_id') AS admin_graphql_api_id,
    JSON_VALUE(imgs, '$.width') AS width,
    JSON_VALUE(imgs, '$.height') AS height,
    JSON_VALUE(imgs, '$.src') AS src,
    JSON_VALUE(imgs, '$.variant_ids') AS variant_ids,

FROM raw_products,
UNNEST(JSON_EXTRACT_ARRAY(images_json)) AS imgs