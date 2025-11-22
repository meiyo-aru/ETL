-- Geração de registros na dimensão de datas (ajuste período conforme necessidade)
INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, day, month_name, weekday_name)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::date AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    TO_CHAR(d, 'TMMonth') AS month_name,
    TO_CHAR(d, 'TMDay') AS weekday_name
FROM generate_series('2023-01-01'::date, '2025-12-31'::date, interval '1 day') AS gs(d)
ON CONFLICT (date_key) DO NOTHING;
