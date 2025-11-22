-- Schema dimensional inicial para DW AdventureWorks
-- Será carregado automaticamente pelo Postgres DW container (montado em /docker-entrypoint-initdb.d)

CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensões
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        INT PRIMARY KEY,        -- formato YYYYMMDD
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    month_name      VARCHAR(20),
    weekday_name    VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INT UNIQUE,             -- chave natural da fonte OLTP
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    full_name       VARCHAR(120),
    gender          VARCHAR(10),
    email           VARCHAR(120),
    country         VARCHAR(60),
    state           VARCHAR(60),
    city            VARCHAR(60),
    postal_code     VARCHAR(20),
    create_date     DATE,
    update_date     DATE
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      INT UNIQUE,
    product_name    VARCHAR(120),
    category        VARCHAR(80),
    subcategory     VARCHAR(80),
    model           VARCHAR(80),
    color           VARCHAR(30),
    size            VARCHAR(20),
    standard_cost   NUMERIC(12,2),
    list_price      NUMERIC(12,2),
    start_date      DATE,
    end_date        DATE
);

CREATE TABLE IF NOT EXISTS dw.dim_territory (
    territory_key   SERIAL PRIMARY KEY,
    territory_id    INT UNIQUE,
    territory_name  VARCHAR(80),
    country_region  VARCHAR(80),
    group_name      VARCHAR(80)
);

CREATE TABLE IF NOT EXISTS dw.dim_sales_person (
    sales_person_key SERIAL PRIMARY KEY,
    sales_person_id  INT UNIQUE,
    full_name        VARCHAR(120),
    territory_key    INT REFERENCES dw.dim_territory(territory_key),
    hire_date        DATE,
    quota            NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS dw.dim_promotion (
    promotion_key   SERIAL PRIMARY KEY,
    promotion_id    INT UNIQUE,
    promotion_name  VARCHAR(120),
    discount_pct    NUMERIC(5,2),
    start_date      DATE,
    end_date        DATE
);

-- Fato principal (Vendas)
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_key       BIGSERIAL PRIMARY KEY,
    date_key        INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_key    INT NOT NULL REFERENCES dw.dim_customer(customer_key),
    product_key     INT NOT NULL REFERENCES dw.dim_product(product_key),
    territory_key   INT REFERENCES dw.dim_territory(territory_key),
    sales_person_key INT REFERENCES dw.dim_sales_person(sales_person_key),
    promotion_key   INT REFERENCES dw.dim_promotion(promotion_key),
    order_id        INT,
    order_line      INT,
    quantity        INT,
    unit_price      NUMERIC(12,2),
    extended_price  NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    total_amount    NUMERIC(14,2),
    cost_amount     NUMERIC(14,2),
    margin_amount   NUMERIC(14,2)
);

-- Índices de performance (exemplos iniciais)
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON dw.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON dw.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON dw.fact_sales(product_key);
