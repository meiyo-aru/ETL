-- Data Quality Checks básicos para DW AdventureWorks

-- 1. Contagem de linhas por tabela dimensão/fato
SELECT 'dim_date' AS tabela, COUNT(*) AS linhas FROM dw.dim_date UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dw.dim_customer UNION ALL
SELECT 'dim_product', COUNT(*) FROM dw.dim_product UNION ALL
SELECT 'dim_territory', COUNT(*) FROM dw.dim_territory UNION ALL
SELECT 'dim_sales_person', COUNT(*) FROM dw.dim_sales_person UNION ALL
SELECT 'dim_promotion', COUNT(*) FROM dw.dim_promotion UNION ALL
SELECT 'fact_sales', COUNT(*) FROM dw.fact_sales;

-- 2. Verificar chaves FK órfãs em fact_sales
SELECT 'orfao_customer' AS tipo, COUNT(*)
FROM dw.fact_sales fs LEFT JOIN dw.dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL
UNION ALL
SELECT 'orfao_product', COUNT(*)
FROM dw.fact_sales fs LEFT JOIN dw.dim_product dp ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL
UNION ALL
SELECT 'orfao_territory', COUNT(*)
FROM dw.fact_sales fs LEFT JOIN dw.dim_territory dt ON fs.territory_key = dt.territory_key
WHERE dt.territory_key IS NULL
UNION ALL
SELECT 'orfao_sales_person', COUNT(*)
FROM dw.fact_sales fs LEFT JOIN dw.dim_sales_person sp ON fs.sales_person_key = sp.sales_person_key
WHERE sp.sales_person_key IS NULL
UNION ALL
SELECT 'orfao_promotion', COUNT(*)
FROM dw.fact_sales fs LEFT JOIN dw.dim_promotion pr ON fs.promotion_key = pr.promotion_key
WHERE pr.promotion_key IS NULL;

-- 3. Campos críticos nulos em fact_sales
SELECT COUNT(*) AS null_date_key FROM dw.fact_sales WHERE date_key IS NULL;
SELECT COUNT(*) AS null_customer_key FROM dw.fact_sales WHERE customer_key IS NULL;
SELECT COUNT(*) AS null_product_key FROM dw.fact_sales WHERE product_key IS NULL;

-- 4. Margens negativas (potenciais anomalias de custo ou preço)
SELECT COUNT(*) AS margens_negativas FROM dw.fact_sales WHERE margin_amount < 0;

-- 5. Distribuição de desconto (faixas)
SELECT CASE 
         WHEN discount_amount = 0 THEN 'Sem desconto'
         WHEN discount_amount < 50 THEN '<50'
         WHEN discount_amount < 200 THEN '50-199'
         ELSE '>=200' END AS faixa,
       COUNT(*) AS linhas
FROM dw.fact_sales
GROUP BY 1
ORDER BY 1;
