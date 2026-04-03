-- import 
with source as (
    SELECT
        "customer_id",
        "signup_date",
        "country",
        "age",
        "gender",
        "loyalty_tier",
        "acquisition_channel"
    FROM {{ source('dw_marketing', 'customers') }}
),

-- renamed
renamed as (
    SELECT
        cast("customer_id" as integer)              as customer_id,
        cast("signup_date" as varchar)               as signup_date,
        cast("country" as varchar)                   as country,
        cast("age" as integer)                       as age,
        cast("gender" as varchar)                    as gender,
        cast("loyalty_tier" as varchar)              as loyalty_tier,
        cast("acquisition_channel" as varchar)       as acquisition_channel
    FROM source
)

SELECT * FROM renamed