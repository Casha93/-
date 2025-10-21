-- HA_clickhouse_marketing_analytics.sql


SET max_memory_usage = 1000000000;
SET max_execution_time = 300;

-- Анализ распределения клиентов по сегментам
INSERT INTO default.HA_marketing_analytics_daily
SELECT
    today() as analysis_date,
    combined_segment as segment,
    COUNT(*) as clients_count,
    SUM(total_value) as total_revenue,
    AVG(total_value) as avg_revenue,
    COUNT(*) as active_clients,
    0 as new_clients,
    now() as created_at
FROM default.HA_marketing_clients
GROUP BY combined_segment;

-- Анализ предложений по типам
SELECT
    offer_type,
    combined_segment,
    COUNT(*) as offers_count,
    SUM(expected_revenue) as total_expected_revenue,
    AVG(expected_revenue) as avg_expected_revenue,
    MIN(days_valid) as min_validity_days,
    MAX(days_valid) as max_validity_days
FROM default.HA_targeted_offers
GROUP BY offer_type, combined_segment
ORDER BY total_expected_revenue DESC;

-- Топ-10 клиентов по ожидаемому доходу от предложений
SELECT
    client_id,
    client_name,
    combined_segment,
    COUNT(*) as offers_count,
    SUM(expected_revenue) as total_expected_revenue,
    AVG(expected_revenue) as avg_offer_value
FROM default.HA_targeted_offers
GROUP BY client_id, client_name, combined_segment
ORDER BY total_expected_revenue DESC
LIMIT 10;

-- Статистика по приоритетам предложений
SELECT
    priority_level,
    offer_priority,
    COUNT(*) as offers_count,
    SUM(expected_revenue) as total_revenue,
    AVG(days_valid) as avg_validity
FROM default.HA_targeted_offers
GROUP BY priority_level, offer_priority
ORDER BY priority_level, offer_priority;

-- Оптимизация таблиц
OPTIMIZE TABLE default.HA_marketing_clients FINAL;
OPTIMIZE TABLE default.HA_targeted_offers FINAL;
OPTIMIZE TABLE default.HA_marketing_analytics_daily FINAL;
