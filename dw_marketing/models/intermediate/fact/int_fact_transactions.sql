{{
    config(
        materialized = 'table',
        unique_key = 'sk_transactions',
        tags = ['intermediate', 'dimension']
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as sk_transactions,

    -- Chave de negócio
    transaction_id,
    
    -- Atributos descritivos
    customer_id,
    product_id,
    quantity,
    discount_applied,
    gross_revenue,
    campaign_id,
    refund_flag,

    -- Datas importantes
    timestamp,
    
    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from transactions