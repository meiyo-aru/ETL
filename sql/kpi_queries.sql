-- Consultas exemplo para KPIs (ajustar após ETL populada)
-- 1. Receita total
SELECT SUM(total_amount) AS receita_total FROM dw.fact_sales;

-- 2. Margem total
SELECT SUM(margin_amount) AS margem_total FROM dw.fact_sales;

-- 3. Ticket médio (valor médio por pedido)
SELECT AVG(total_por_pedido) AS ticket_medio
FROM (
  SELECT order_id, SUM(total_amount) AS total_por_pedido
  FROM dw.fact_sales
  GROUP BY order_id
) t;

-- 4. Quantidade de clientes ativos (clientes que compraram nos últimos 12 meses do período disponível)
SELECT COUNT(DISTINCT customer_key) AS clientes_ativos
FROM dw.fact_sales fs
JOIN dw.dim_date d ON fs.date_key = d.date_key
WHERE d.full_date >= (
    SELECT MAX(full_date) - INTERVAL '12 months' 
    FROM dw.fact_sales fs2 
    JOIN dw.dim_date d2 ON fs2.date_key = d2.date_key
);

-- 5. Top 10 produtos por receita
SELECT p.product_name, SUM(fs.total_amount) AS receita
FROM dw.fact_sales fs
JOIN dw.dim_product p ON fs.product_key = p.product_key
GROUP BY p.product_name
ORDER BY receita DESC
LIMIT 10;

-- 6. Conversão de promoção (receita gerada por vendas com promoção / receita total)
SELECT
  (SUM(CASE WHEN promotion_key IS NOT NULL THEN total_amount ELSE 0 END) / NULLIF(SUM(total_amount),0)) * 100 AS pct_receita_promocao
FROM dw.fact_sales;

-- 7. Margem média por categoria
SELECT p.category, AVG(fs.margin_amount) AS margem_media
FROM dw.fact_sales fs
JOIN dw.dim_product p ON fs.product_key = p.product_key
GROUP BY p.category;

-- 8. Performance vendedor (receita vs quota)
SELECT sp.full_name,
       SUM(fs.total_amount) AS receita_vendedor,
       sp.quota,
       (SUM(fs.total_amount) / NULLIF(sp.quota,0))*100 AS pct_quota
FROM dw.fact_sales fs
JOIN dw.dim_sales_person sp ON fs.sales_person_key = sp.sales_person_key
GROUP BY sp.full_name, sp.quota;

-- 9. Crescimento mensal de receita (últimos 6 meses)
SELECT d.year, d.month, SUM(fs.total_amount) AS receita_mes
FROM dw.fact_sales fs
JOIN dw.dim_date d ON fs.date_key = d.date_key
WHERE d.full_date >= (date_trunc('month', CURRENT_DATE) - INTERVAL '6 months')
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 10. Margem média por região
SELECT t.country_region, AVG(fs.margin_amount) AS margem_media
FROM dw.fact_sales fs
JOIN dw.dim_territory t ON fs.territory_key = t.territory_key
GROUP BY t.country_region;
