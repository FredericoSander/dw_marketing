{{
    config(
        materialized = 'table',
        unique_key = 'sk_customers',
        tags = ['intermediate', 'dimension']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as sk_customers,
    
    -- Chave de negócio
    customer_id,
    
    -- Atributos descritivos
    signup_date,
    country,
    age,
    gender,
    loyalty_tier,
    acquisition_channel,

    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from customers