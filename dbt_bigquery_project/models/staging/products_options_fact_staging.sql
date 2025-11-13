WITH raw_products AS (
    SELECT
        id as product_id,
        REPLACE(
            REPLACE(
                REPLACE(options, "'", '"'),
                "None", "null"
            ),
            "True", "true"
            ) as opts_json
    FROM {{ source('raw_data','products_raw') }}
)


SELECT
    JSON_VALUE(optns, '$.id') AS product_options_id,
    product_id,
    JSON_VALUE(optns, '$.name') AS `name`,
    JSON_VALUE(optns, '$.position') AS position,
    JSON_VALUE(optns, '$.values') AS created_at,

FROM raw_products,
UNNEST(JSON_EXTRACT_ARRAY(opts_json)) AS optns