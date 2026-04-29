{{
    config(
        materialized = 'table',
        unique_key = 'campaign_id',
        tags = ['mart', 'metrics']
    )
}}

WITH metricas_campanha AS (
    -- Faturamento por campanha e país
    SELECT
        COALESCE(c.campaign_id, 0) AS campanha_id,
        c.channel AS canal,
        c.objective AS objetivo,
        c.target_segment AS segmento_alvo,
        COUNT(DISTINCT t.customer_id)::INT AS clientes_unicos,
        COUNT(t.transaction_id)::INT AS total_transacoes,
        ROUND(SUM(t.quantity)::NUMERIC, 2)::NUMERIC(18,2) AS quantidade_itens_vendidos,
        ROUND(SUM(t.gross_revenue)::NUMERIC, 2)::NUMERIC(18,2) AS receita_total,
        ROUND(AVG(t.gross_revenue)::NUMERIC, 2)::NUMERIC(18,2) AS ticket_medio,
        ROUND(SUM(t.gross_revenue)::NUMERIC / NULLIF(COUNT(DISTINCT t.customer_id), 0), 2)::NUMERIC(18,2) AS receita_por_cliente,
        ROW_NUMBER() OVER (ORDER BY SUM(t.gross_revenue) DESC)::INT AS sk_campaigns,
        CURRENT_TIMESTAMP AS data_carga
    FROM {{ ref('int_fact_transactions') }} t
    LEFT JOIN {{ ref('int_dim_campaigns') }} c ON t.campaign_id = c.campaign_id
    GROUP BY COALESCE(c.campaign_id, 0), c.channel, c.objective, c.target_segment
),

eventos_por_campanha AS (
    SELECT
        campaign_id AS campanha_id,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END)::INT AS total_adicionar_ao_carrinho,
        COUNT(CASE WHEN event_type = 'bounce'      THEN 1 END)::INT AS total_pular,
        COUNT(CASE WHEN event_type = 'click'       THEN 1 END)::INT AS total_cliques,
        COUNT(CASE WHEN event_type = 'purchase'    THEN 1 END)::INT AS total_compras,
        COUNT(CASE WHEN event_type = 'view'        THEN 1 END)::INT AS total_visualizacoes
    FROM {{ ref('int_dim_events') }}
    GROUP BY campaign_id
),

ranking_transacoes AS (
    SELECT
	c.campaign_id AS campanha_id,
	RANK()OVER(ORDER BY COUNT(t.transaction_id) DESC) as ranking_transacoes,
    RANK()OVER(ORDER BY SUM(t.quantity) DESC) as ranking_quantidade,
	RANK()OVER(ORDER BY SUM(t.gross_revenue) DESC) as ranking_receita_total,
	RANK()OVER(ORDER BY SUM(t.refund_flag) DESC) as ranking_reembolsos
    FROM {{ ref('int_fact_transactions') }} t
    LEFT JOIN {{ ref('int_dim_campaigns') }} c ON t.campaign_id = c.campaign_id
    WHERE c.campaign_id IS NOT NULL
    GROUP BY c.campaign_id
)

SELECT
    cm.campanha_id,
    cm.canal,
    cm.objetivo,
    cm.segmento_alvo,
    cm.clientes_unicos,
    cm.total_transacoes,
    cm.quantidade_itens_vendidos,
    cm.receita_total,
    cm.ticket_medio,
    cm.receita_por_cliente,
    ev.total_adicionar_ao_carrinho,
    ev.total_pular,
    ev.total_cliques,
    ev.total_compras,
    ev.total_visualizacoes,
    rt.ranking_transacoes,
    rt.ranking_quantidade,
    rt.ranking_receita_total,
    rt.ranking_reembolsos,
    cm.data_carga
FROM metricas_campanha cm
LEFT JOIN ranking_transacoes rt ON cm.campanha_id = rt.campanha_id
LEFT JOIN eventos_por_campanha ev ON cm.campanha_id = ev.campanha_id
ORDER BY cm.receita_total DESC