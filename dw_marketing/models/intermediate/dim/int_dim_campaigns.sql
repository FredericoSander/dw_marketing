-- import 

{{
    config(
        materialized = 'table',
        unique_key = 'sk_campaigns',
        tags = ['intermediate', 'dimension']
    )
}}

with campaigns as (
    select * from {{ ref('stg_campaigns') }}
)


SELECT
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as sk_campaigns,
    
    -- Chave de negócio
    campaign_id,
    
    -- Atributos descritivos
    channel,
    objective,
    start_date,
    end_date,
    target_segment,
    expected_uplift,
           
    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
FROM campaigns