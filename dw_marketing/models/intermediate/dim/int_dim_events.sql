{{
    config(
        materialized = 'table',
        unique_key = 'sk_events',
        tags = ['intermediate', 'dimension']
    )
}}

with events as (
    select * from {{ ref('stg_events') }}
)

select
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as sk_events,

    -- Chave de negócio
    event_id,

    -- Atributos descritivos
    customer_id,
    session_id,
    event_type,
    product_id,
    device_type,
    traffic_source,
    campaign_id,
    page_category,
    session_duration_sec,
    experiment_group
 
    -- Datas importantes
    timestamp,
    
    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from events