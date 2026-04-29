{{
    config(
        materialized = 'table',
        unique_key = 'customers_id',
        tags = ['mart', 'metrics']
    )
}}

WITH customer_metrics AS (
    -- Faturamento por campanha e país
    SELECT
        cu.customer_id,
        --Metricas de contagem
        COUNT(DISTINCT t.transaction_id) AS total_de_transacoes,
        --Metricas financeiras
        SUM(t.gross_revenue)::NUMERIC(18,2) AS valor_total,
        AVG(t.gross_revenue)::NUMERIC(18,2) AS ticket_medio,
        -- Análise temporal
        MIN(t.timestamp) AS data_do_primeiro_pedido,
        MAX(t.timestamp) AS data_do_ultimo_pedido,
        COUNT(DISTINCT dd.ano) AS total_anos_ativos,
        -- Estacionalidade
        COUNT(DISTINCT CASE WHEN dd.nome_mes in ('December','January','February') THEN t.transaction_id END) AS pedidos_verao,
        COUNT(DISTINCT CASE WHEN dd.nome_mes in ('March', 'April', 'May') THEN t.transaction_id END) AS pedidos_outono,
        COUNT(DISTINCT CASE WHEN dd.nome_mes in ('June', 'July', 'August') THEN t.transaction_id END) AS pedidos_inverno,
        COUNT(DISTINCT CASE WHEN dd.nome_mes in ('September', 'October', 'November') THEN t.transaction_id END) AS pedidos_primavera,
        -- Dias da semana com mais compras
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Sunday' THEN t.transaction_id END) AS pedidos_domingo,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Monday' THEN t.transaction_id END) AS pedidos_segunda,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Tuesday' THEN t.transaction_id END) AS pedidos_terca,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Wednesday' THEN t.transaction_id END) AS pedidos_quarta,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Thursday' THEN t.transaction_id END) AS pedidos_quinta,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Friday' THEN t.transaction_id END) AS pedidos_sexta,
        COUNT(DISTINCT CASE WHEN dd.nome_dia = 'Saturday' THEN t.transaction_id END) AS pedidos_sabado,
        -- transações em campanhas e sem campanhas
	    COUNT(DISTINCT CASE WHEN t.campaign_id = 0 THEN t.transaction_id END) AS transacao_sem_campanhas,
	    COUNT(DISTINCT CASE WHEN t.campaign_id > 0 THEN t.transaction_id END) AS transacao_com_campanhas,
	    -- Receita com e sem campanha
	    SUM(CASE WHEN t.campaign_id = 0 THEN t.gross_revenue ELSE 0 END)::NUMERIC(18,2) AS receita_sem_campanhas,
        SUM(CASE WHEN t.campaign_id > 0 THEN t.gross_revenue ELSE 0 END)::NUMERIC(18,2) AS receita_com_campanhas
    FROM {{ ref('int_dim_customers') }} cu
    LEFT JOIN {{ ref('int_fact_transactions') }} t ON t.customer_id = cu.customer_id
    LEFT JOIN {{ ref('int_dim_date') }} dd ON dd.data = t.timestamp::date
    GROUP BY cu.customer_id
    HAVING COUNT(DISTINCT t.transaction_id) > 0
    ORDER BY customer_id
)

SELECT
    cm.customer_id,
    cm.total_de_transacoes,
    cm.valor_total,
    cm.ticket_medio,
    cm.data_do_primeiro_pedido,
    cm.data_do_ultimo_pedido,
    cm.total_anos_ativos,
    cm.pedidos_verao,
    cm.pedidos_outono,
    cm.pedidos_inverno,
    cm.pedidos_primavera,
    cm.transacao_sem_campanhas,
    cm.transacao_com_campanhas,
    cm.receita_sem_campanhas,
    cm.receita_com_campanhas
FROM customer_metrics cm
-- LEFT JOIN ranking_transactions rt ON cm.campaign_id = rt.campaign_id
ORDER BY cm.customer_id DESC