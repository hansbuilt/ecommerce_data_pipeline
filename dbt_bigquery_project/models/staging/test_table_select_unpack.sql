WITH raw_customers AS (
    SELECT
        id as customer_id,
        REPLACE(
            REPLACE(
                REPLACE(addresses, "'", '"'),
                "None", "null"
            ),
            "True", "true"
            ) as addresses_json
    FROM {{ source('raw_data','customers_raw') }}
)


SELECT
    customer_id,
    JSON_VALUE(addr, '$.id') AS customer_address_id,
    JSON_VALUE(addr, '$.first_name') AS first_name,
    JSON_VALUE(addr, '$.last_name') AS last_name,
    JSON_VALUE(addr, '$.company') AS company,
    JSON_VALUE(addr, '$.address1') AS address1,
    JSON_VALUE(addr, '$.address2') AS address2,
    JSON_VALUE(addr, '$.city') AS city,
    JSON_VALUE(addr, '$.province') AS `state`,
    JSON_VALUE(addr, '$.zip') AS zip,
    JSON_VALUE(addr, '$.phone') AS phone,
    JSON_VALUE(addr, '$.name') AS `name`,
    JSON_VALUE(addr, '$.province_code') AS `state_code`,
    JSON_VALUE(addr, '$.country_code') AS country_code,
    JSON_VALUE(addr, '$.country_name') AS country_name,
    JSON_VALUE(addr, '$.default') AS `is_address_default`
FROM raw_customers,
UNNEST(JSON_EXTRACT_ARRAY(addresses_json)) AS addr