-- models/intermediate/dim/int_dim_date.sql
-- Dimensão de Datas - Estrutura completa para análise temporal

WITH date_range AS (
    SELECT
        GENERATE_SERIES(
            '2021-01-01'::DATE,
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE AS data
)

SELECT
    -- Chaves e identificadores
    data::TEXT AS date_key,
    data,
    
    -- Decomposição temporal - Ano
    EXTRACT(YEAR FROM data)::INT AS ano,
    TO_CHAR(data, 'YYYY')::VARCHAR(4) AS ano_text,
    
    -- Decomposição temporal - Mês
    EXTRACT(MONTH FROM data)::INT AS mes,
    TO_CHAR(data, 'MM')::VARCHAR(2) AS mes_zero_padded,
    TO_CHAR(data, 'Month')::VARCHAR(20) AS nome_mes,
    TO_CHAR(data, 'Mon')::VARCHAR(3) AS nome_mes_abreviado,
    
    -- Decomposição temporal - Trimestre
    EXTRACT(QUARTER FROM data)::INT AS trimestre,
    CONCAT('Q', EXTRACT(QUARTER FROM data))::VARCHAR(2) AS trimestre_text,
    
    -- Decomposição temporal - Semana
    EXTRACT(WEEK FROM data)::INT AS semana_iso,
    EXTRACT(ISODOW FROM data)::INT AS dia_semana_iso,
    
    -- Decomposição temporal - Dia
    EXTRACT(DAY FROM data)::INT AS dia,
    TO_CHAR(data, 'DD')::VARCHAR(2) AS dia_zero_padded,
    EXTRACT(DOY FROM data)::INT AS dia_ano,
    
    -- Nome do dia da semana
    TO_CHAR(data, 'Day')::VARCHAR(10) AS nome_dia,
    TO_CHAR(data, 'Dy')::VARCHAR(3) AS nome_dia_abreviado,
    EXTRACT(DOW FROM data)::INT AS dia_semana_pg,  -- 0=Sunday, 6=Saturday
    
    -- Indicadores booleanos
    CASE 
        WHEN EXTRACT(DOW FROM data) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS eh_fim_de_semana,
    
    CASE 
        WHEN EXTRACT(DOW FROM data) = 0 THEN TRUE
        ELSE FALSE
    END AS eh_domingo,
    
    CASE 
        WHEN EXTRACT(DOW FROM data) = 6 THEN TRUE
        ELSE FALSE
    END AS eh_sabado,
    
    -- Estação (Hemisfério Sul - Brasil)
    CASE 
        WHEN EXTRACT(MONTH FROM data) IN (12, 1, 2) THEN 'Verão'
        WHEN EXTRACT(MONTH FROM data) IN (3, 4, 5) THEN 'Outono'
        WHEN EXTRACT(MONTH FROM data) IN (6, 7, 8) THEN 'Inverno'
        WHEN EXTRACT(MONTH FROM data) IN (9, 10, 11) THEN 'Primavera'
    END AS estacao,
    
    -- Formatações úteis
    TO_CHAR(data, 'YYYY-MM-DD')::VARCHAR(10) AS data_formatada_iso,
    TO_CHAR(data, 'DD/MM/YYYY')::VARCHAR(10) AS data_formatada_br,
    TO_CHAR(data, 'Month DD, YYYY')::VARCHAR(30) AS data_formatada_completa_en,
    
    -- Período fiscal (ajuste conforme sua política fiscal)
    CONCAT(
        EXTRACT(YEAR FROM data),
        '-',
        EXTRACT(QUARTER FROM data)
    )::VARCHAR(6) AS periodo_fiscal,
    
    -- Metadados
    CURRENT_TIMESTAMP AS data_carga,
    'DBT' AS origem_dados

FROM date_range

ORDER BY data