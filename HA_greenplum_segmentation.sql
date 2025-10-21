
-- HA_greenplum_segmentation.sql - PRODUCTION VERSION

-- Шаг 1: Загрузка данных с дедупликацией


-- 1.1 Клиенты (оставляем самую свежую запись по client_id)
TRUNCATE TABLE hryapin.ha_clients_structured;
INSERT INTO hryapin.ha_clients_structured
SELECT
    client_id,
    client_name,
    phone,
    email,
    birth_date,
    registration_date,
    city,
    segment,
    status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY registration_date DESC NULLS LAST, client_name) AS rn
    FROM hryapin.ext_ha_clients_structured
) t
WHERE rn = 1;

-- 1.2 Транзакции
TRUNCATE TABLE hryapin.ha_bank_transactions_structured;
INSERT INTO hryapin.ha_bank_transactions_structured
SELECT
    transaction_id,
    client_id,
    transaction_date,
    amount,
    currency,
    transaction_type,
    channel
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date DESC NULLS LAST) AS rn
    FROM hryapin.ext_ha_bank_transactions_structured
) t
WHERE rn = 1;

-- 1.3 Логины
TRUNCATE TABLE hryapin.ha_client_logins_structured;
INSERT INTO hryapin.ha_client_logins_structured
SELECT
    login_id,
    client_id,
    login_timestamp,
    ip_address,
    device_type,
    browser,
    success
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY login_id ORDER BY login_timestamp DESC NULLS LAST) AS rn
    FROM hryapin.ext_ha_client_logins_structured
) t
WHERE rn = 1;

-- 1.4 Активности
TRUNCATE TABLE hryapin.ha_client_activities_structured;
INSERT INTO hryapin.ha_client_activities_structured
SELECT
    activity_id,
    client_id,
    activity_date,
    activity_type,
    duration_minutes,
    page_views
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY activity_date DESC NULLS LAST) AS rn
    FROM hryapin.ext_ha_client_activities_structured
) t
WHERE rn = 1;

-- 1.5 Платежи
TRUNCATE TABLE hryapin.ha_payments_structured;
INSERT INTO hryapin.ha_payments_structured
SELECT
    payment_id,
    client_id,
    payment_date,
    amount,
    currency,
    payment_type,
    status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_date DESC NULLS LAST) AS rn
    FROM hryapin.ext_ha_payments_structured
) t
WHERE rn = 1;

-- 1.6 Портфели
TRUNCATE TABLE hryapin.ha_portfolios_structured;
INSERT INTO hryapin.ha_portfolios_structured
SELECT
    portfolio_id,
    client_id,
    security_type,
    security_name,
    quantity,
    purchase_price,
    current_price,
    purchase_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY portfolio_id ORDER BY purchase_date DESC NULLS LAST) AS rn
    FROM hryapin.ext_ha_portfolios_structured
) t
WHERE rn = 1;


-- Шаг 2: Создание промежуточных агрегатов (оптимизация)


DROP TABLE IF EXISTS hryapin.tmp_transactions_agg CASCADE;
CREATE TEMP TABLE tmp_transactions_agg AS
SELECT
    client_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_transaction_amount,
    AVG(amount) AS avg_transaction_amount,
    MAX(amount) AS max_transaction_amount
FROM hryapin.ha_bank_transactions_structured
GROUP BY client_id
DISTRIBUTED BY (client_id);

DROP TABLE IF EXISTS hryapin.tmp_logins_agg CASCADE;
CREATE TEMP TABLE tmp_logins_agg AS
SELECT
    client_id,
    COUNT(*) AS login_count,
    COUNT(*) FILTER (WHERE success = true) AS successful_logins
FROM hryapin.ha_client_logins_structured
GROUP BY client_id
DISTRIBUTED BY (client_id);

DROP TABLE IF EXISTS hryapin.tmp_activities_agg CASCADE;
CREATE TEMP TABLE tmp_activities_agg AS
SELECT
    client_id,
    COUNT(*) AS activity_count,
    SUM(duration_minutes) AS total_activity_minutes,
    SUM(page_views) AS total_page_views
FROM hryapin.ha_client_activities_structured
GROUP BY client_id
DISTRIBUTED BY (client_id);

DROP TABLE IF EXISTS hryapin.tmp_payments_agg CASCADE;
CREATE TEMP TABLE tmp_payments_agg AS
SELECT
    client_id,
    COUNT(*) AS payment_count,
    SUM(amount) AS total_payment_amount
FROM hryapin.ha_payments_structured
GROUP BY client_id
DISTRIBUTED BY (client_id);

DROP TABLE IF EXISTS hryapin.tmp_portfolios_agg CASCADE;
CREATE TEMP TABLE tmp_portfolios_agg AS
SELECT
    client_id,
    COUNT(*) AS portfolio_count,
    SUM(quantity * current_price) AS portfolio_value
FROM hryapin.ha_portfolios_structured
GROUP BY client_id
DISTRIBUTED BY (client_id);


-- Шаг 3: Финальная сегментация с ВЫЧИСЛЕНИЕМ SEGMENT


DROP TABLE IF EXISTS hryapin.ha_client_segmentation CASCADE;

CREATE TABLE hryapin.ha_client_segmentation AS
SELECT
    c.client_id,
    c.client_name,
    c.city,
    c.status,


    CASE
        -- VIP: высокая сумма транзакций + активность ИЛИ большой портфель
        WHEN (COALESCE(t.total_transaction_amount, 0) >= 200000 AND COALESCE(l.login_count, 0) >= 5)
             OR COALESCE(pf.portfolio_value, 0) >= 500000
        THEN 'VIP'

        -- Premium: средняя сумма транзакций ИЛИ высокая активность
        WHEN COALESCE(t.total_transaction_amount, 0) >= 100000
             OR (COALESCE(l.login_count, 0) >= 10 AND COALESCE(a.activity_count, 0) >= 20)
        THEN 'Premium'

        -- Standard: небольшая активность
        WHEN COALESCE(t.total_transaction_amount, 0) >= 50000
        THEN 'Standard'

        -- Basic: всё остальное
        ELSE 'Basic'
    END AS segment,

    c.segment AS original_segment,

    -- Транзакционная активность
    COALESCE(t.transaction_count, 0) AS transaction_count,
    COALESCE(t.total_transaction_amount, 0) AS total_transaction_amount,
    COALESCE(t.avg_transaction_amount, 0) AS avg_transaction_amount,
    COALESCE(t.max_transaction_amount, 0) AS max_transaction_amount,

    -- Активность входов
    COALESCE(l.login_count, 0) AS login_count,
    COALESCE(l.successful_logins, 0) AS successful_logins,

    -- Общая активность
    COALESCE(a.activity_count, 0) AS activity_count,
    COALESCE(a.total_activity_minutes, 0) AS total_activity_minutes,
    COALESCE(a.total_page_views, 0) AS total_page_views,

    -- Платежи
    COALESCE(p.payment_count, 0) AS payment_count,
    COALESCE(p.total_payment_amount, 0) AS total_payment_amount,

    -- Портфель
    COALESCE(pf.portfolio_count, 0) AS portfolio_count,
    COALESCE(pf.portfolio_value, 0) AS portfolio_value,

    -- Метаданные
    CURRENT_DATE AS processing_date,
    CURRENT_TIMESTAMP AS created_at

FROM hryapin.ha_clients_structured c
LEFT JOIN tmp_transactions_agg t ON c.client_id = t.client_id
LEFT JOIN tmp_logins_agg l ON c.client_id = l.client_id
LEFT JOIN tmp_activities_agg a ON c.client_id = a.client_id
LEFT JOIN tmp_payments_agg p ON c.client_id = p.client_id
LEFT JOIN tmp_portfolios_agg pf ON c.client_id = pf.client_id
DISTRIBUTED BY (client_id);

-- Индексы для оптимизации запросов
CREATE INDEX idx_segmentation_segment ON hryapin.ha_client_segmentation(segment);
CREATE INDEX idx_segmentation_city ON hryapin.ha_client_segmentation(city);
CREATE INDEX idx_segmentation_status ON hryapin.ha_client_segmentation(status);

-- Обновление статистики
ANALYZE hryapin.ha_client_segmentation;


-- Verification: Проверка результатов


-- Общая статистика
SELECT
    'Total clients' AS metric,
    COUNT(*) AS value
FROM hryapin.ha_client_segmentation
UNION ALL
SELECT
    'Unique client_ids' AS metric,
    COUNT(DISTINCT client_id) AS value
FROM hryapin.ha_client_segmentation
UNION ALL
-- Распределение по ВЫЧИСЛЕННЫМ сегментам
SELECT
    'Calculated Segment: ' || segment AS metric,
    COUNT(*) AS value
FROM hryapin.ha_client_segmentation
GROUP BY segment
UNION ALL
-- Распределение по ОРИГИНАЛЬНЫМ сегментам (из генератора)
SELECT
    'Original Segment: ' || original_segment AS metric,
    COUNT(*) AS value
FROM hryapin.ha_client_segmentation
GROUP BY original_segment
ORDER BY metric;

-- Сравнение: сколько клиентов изменили сегмент
SELECT
    'Segments matched' AS comparison,
    COUNT(*) AS clients
FROM hryapin.ha_client_segmentation
WHERE segment = original_segment
UNION ALL
SELECT
    'Segments changed',
    COUNT(*)
FROM hryapin.ha_client_segmentation
WHERE segment != original_segment;