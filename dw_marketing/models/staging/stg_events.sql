-- import 
with source as (
    SELECT
        "event_id",
        "timestamp",
        "customer_id",
        "session_id",
        "event_type",
        "product_id",
        "device_type",
        "traffic_source",
        "campaign_id",
        "page_category",
        "session_duration_sec",
        "experiment_group"
    FROM {{ source('dw_marketing', 'events') }}
),

-- renamed
renamed as (
    SELECT
        cast("event_id" as integer)                         as event_id,
        cast("timestamp" as timestamp)                      as timestamp,
        cast("customer_id" as integer)                      as customer_id,
        cast("session_id" as integer)                       as session_id,
        cast("event_type" as varchar)                       as event_type,
        cast("product_id" as integer)                       as product_id,
        cast("device_type" as varchar)                      as device_type,
        cast("traffic_source" as varchar)                   as traffic_source,
        cast("campaign_id" as integer)                      as campaign_id,
        cast("page_category" as varchar)                    as page_category,
        cast("session_duration_sec" as double precision)    as session_duration_sec,
        cast("experiment_group" as varchar)                 as experiment_group
    FROM source
)

SELECT * FROM renamed       