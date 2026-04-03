-- import 
with source as (
    SELECT
        "campaign_id",
        "channel",
        "objective",
        "start_date",
        "end_date",
        "target_segment",
        "expected_uplift"
    FROM {{ source('dw_marketing', 'campaigns') }}
),

-- renamed
renamed as (
    SELECT
        cast("campaign_id" as integer)              as campaign_id,
        cast("channel" as varchar)                  as channel,
        cast("objective" as varchar)                as objective,
        cast("start_date" as date)                  as start_date,
        cast("end_date" as date)                    as end_date,
        cast("target_segment" as varchar)           as target_segment,
        cast("expected_uplift" as double precision) as expected_uplift
    FROM source
)

SELECT * FROM renamed