-- import 
with source as (
    SELECT
        "product_id",
        "category",
        "brand",
        "base_price",
        "launch_date",
        "is_premium"
    FROM {{ source('dw_marketing', 'products') }}
),

-- renamed
renamed as (
    SELECT
        cast("product_id" as integer)                as product_id,
        cast("category" as varchar)                  as category,
        cast("brand" as varchar)                     as brand,
        cast("base_price" as double precision)       as base_price,
        cast("launch_date" as date)                  as launch_date,
        cast("is_premium" as integer)                as is_premium
    FROM source
)

SELECT * FROM renamed