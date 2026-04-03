-- import 
with source as (
    SELECT
        "transaction_id",
        "timestamp",
        "customer_id",
        "product_id",
        "quantity",
        "discount_applied",
        "gross_revenue",
        "campaign_id",
        "refund_flag"
    FROM {{ source('dw_marketing', 'transactions') }}
),

-- renamed
renamed as (
    SELECT
        cast("transaction_id" as integer)             as transaction_id,
        cast("timestamp" as timestamp)                as timestamp,
        cast("customer_id" as integer)                as customer_id,
        cast("product_id" as integer)                 as product_id,
        cast("quantity" as integer)                   as quantity,
        cast("discount_applied" as double precision)   as discount_applied,
        cast("gross_revenue" as double precision)     as gross_revenue,
        cast("campaign_id" as integer)                as campaign_id,
        cast("refund_flag" as integer)                as refund_flag
    FROM source
)

SELECT * FROM renamed
