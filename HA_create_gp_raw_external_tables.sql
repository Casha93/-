
-- HA_create_gp_raw_external_tables.sql


CREATE SCHEMA IF NOT EXISTS hryapin;


-- 1. КЛИЕНТЫ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_clients_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_clients_structured (
    client_id VARCHAR(20),
    client_name VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100),
    birth_date DATE,
    registration_date DATE,
    city VARCHAR(100),
    segment VARCHAR(50),
    status VARCHAR(20)
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_clients_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_clients_structured CASCADE;
CREATE TABLE hryapin.ha_clients_structured (
    client_id VARCHAR(20) NOT NULL,
    client_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(100),
    birth_date DATE,
    registration_date DATE,
    city VARCHAR(100),
    segment VARCHAR(50),
    status VARCHAR(20)
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 2. ТРАНЗАКЦИИ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_bank_transactions_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_bank_transactions_structured (
    transaction_id VARCHAR(20),
    client_id VARCHAR(20),
    transaction_date DATE,
    amount DOUBLE PRECISION,
    currency VARCHAR(3),
    transaction_type VARCHAR(50),
    channel VARCHAR(50)
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_bank_transactions_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_bank_transactions_structured CASCADE;
CREATE TABLE hryapin.ha_bank_transactions_structured (
    transaction_id VARCHAR(20) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    transaction_date DATE NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    channel VARCHAR(50)
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 3. ЛОГИНЫ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_client_logins_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_client_logins_structured (
    login_id VARCHAR(20),
    client_id VARCHAR(20),
    login_timestamp TIMESTAMP,
    ip_address VARCHAR(45),
    device_type VARCHAR(50),
    browser VARCHAR(100),
    success BOOLEAN
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_client_logins_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_client_logins_structured CASCADE;
CREATE TABLE hryapin.ha_client_logins_structured (
    login_id VARCHAR(20) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    login_timestamp TIMESTAMP NOT NULL,
    ip_address VARCHAR(45),
    device_type VARCHAR(50),
    browser VARCHAR(100),
    success BOOLEAN
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 4. АКТИВНОСТИ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_client_activities_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_client_activities_structured (
    activity_id VARCHAR(20),
    client_id VARCHAR(20),
    activity_date DATE,
    activity_type VARCHAR(100),
    duration_minutes INTEGER,
    page_views INTEGER
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_client_activities_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_client_activities_structured CASCADE;
CREATE TABLE hryapin.ha_client_activities_structured (
    activity_id VARCHAR(20) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    activity_date DATE NOT NULL,
    activity_type VARCHAR(100) NOT NULL,
    duration_minutes INTEGER,
    page_views INTEGER
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 5. ПЛАТЕЖИ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_payments_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_payments_structured (
    payment_id VARCHAR(20),
    client_id VARCHAR(20),
    payment_date DATE,
    amount DOUBLE PRECISION,
    currency VARCHAR(3),
    payment_type VARCHAR(50),
    status VARCHAR(20)
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_payments_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_payments_structured CASCADE;
CREATE TABLE hryapin.ha_payments_structured (
    payment_id VARCHAR(20) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    payment_date DATE NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    currency VARCHAR(3) NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    status VARCHAR(20)
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 6. ПОРТФЕЛИ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_portfolios_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_portfolios_structured (
    portfolio_id VARCHAR(20),
    client_id VARCHAR(20),
    security_type VARCHAR(50),
    security_name VARCHAR(200),
    quantity INTEGER,
    purchase_price DOUBLE PRECISION,
    current_price DOUBLE PRECISION,
    purchase_date DATE
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_portfolios_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_portfolios_structured CASCADE;
CREATE TABLE hryapin.ha_portfolios_structured (
    portfolio_id VARCHAR(20) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    security_type VARCHAR(50),
    security_name VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL,
    purchase_price DOUBLE PRECISION NOT NULL,
    current_price DOUBLE PRECISION,
    purchase_date DATE
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (client_id);


-- 7. КУРСЫ ВАЛЮТ: Внешняя таблица → Локальная таблица

DROP EXTERNAL TABLE IF EXISTS hryapin.ext_ha_currency_rates_structured CASCADE;
CREATE EXTERNAL TABLE hryapin.ext_ha_currency_rates_structured (
    rate_id VARCHAR(20),
    currency_pair VARCHAR(10),
    rate_date TIMESTAMP,
    rate_value DOUBLE PRECISION,
    rate_type VARCHAR(50)
)
LOCATION ('pxf://user/a.hryapin/ha_cleaned/ha_currency_rates_structured/*?PROFILE=hdfs:parquet&SERVER=default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP TABLE IF EXISTS hryapin.ha_currency_rates_structured CASCADE;
CREATE TABLE hryapin.ha_currency_rates_structured (
    rate_id VARCHAR(20) NOT NULL,
    currency_pair VARCHAR(10) NOT NULL,
    rate_date TIMESTAMP NOT NULL,
    rate_value DOUBLE PRECISION NOT NULL,
    rate_type VARCHAR(50)
)
WITH (APPENDOPTIMIZED=TRUE, COMPRESSTYPE=ZSTD, COMPRESSLEVEL=3)
DISTRIBUTED BY (rate_id);


-- Verification query для external tables

SELECT 'Clients (EXT)' AS table_name, COUNT(*) FROM hryapin.ext_ha_clients_structured
UNION ALL
SELECT 'Transactions (EXT)', COUNT(*) FROM hryapin.ext_ha_bank_transactions_structured
UNION ALL
SELECT 'Logins (EXT)', COUNT(*) FROM hryapin.ext_ha_client_logins_structured
UNION ALL
SELECT 'Activities (EXT)', COUNT(*) FROM hryapin.ext_ha_client_activities_structured
UNION ALL
SELECT 'Payments (EXT)', COUNT(*) FROM hryapin.ext_ha_payments_structured
UNION ALL
SELECT 'Portfolios (EXT)', COUNT(*) FROM hryapin.ext_ha_portfolios_structured
UNION ALL
SELECT 'Currency Rates (EXT)', COUNT(*) FROM hryapin.ext_ha_currency_rates_structured;