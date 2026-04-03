{{
    config(
        materialized = 'table',
        unique_key = 'sk_products',
        tags = ['intermediate', 'dimension']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
)

select
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as sk_products,
    
    -- Chave de negócio
    product_id,
    
    -- Atributos descritivos
    category,
    brand,
    base_price,
    is_premium,

    -- Datas importantes
    launch_date,
    
    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from products

