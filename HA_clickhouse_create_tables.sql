-- HA_clickhouse_create_tables.sql
-- Создание таблиц ClickHouse для HA проекта


CREATE DATABASE IF NOT EXISTS default;

-- Таблица для клиентов с сегментацией
DROP TABLE IF EXISTS default.HA_marketing_clients;
CREATE TABLE default.HA_marketing_clients
(
    client_id String,
    client_name LowCardinality(String),
    portfolio_segment LowCardinality(String),
    transaction_segment LowCardinality(String),
    combined_segment LowCardinality(String),
    total_value Decimal(15, 2),
    activity_score UInt32,
    priority_level UInt8,
    last_updated DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_updated)
PARTITION BY toYYYYMM(last_updated)
ORDER BY (client_id, priority_level)
SETTINGS index_granularity = 8192;

-- Таблица для таргетированных предложений
DROP TABLE IF EXISTS default.HA_targeted_offers;
CREATE TABLE default.HA_targeted_offers
(
    offer_id UInt64,
    client_id String,
    client_name LowCardinality(String),
    portfolio_segment LowCardinality(String),
    transaction_segment LowCardinality(String),
    combined_segment LowCardinality(String),
    offer_type LowCardinality(String),
    offer_description String,
    expected_revenue Decimal(12, 2),
    priority_level UInt8,
    offer_priority UInt8,
    days_valid UInt16,
    total_portfolio_value Decimal(15, 2),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (client_id, priority_level, offer_priority, created_at)
SETTINGS index_granularity = 8192;

-- Таблица для результатов маркетинговых кампаний
DROP TABLE IF EXISTS default.HA_marketing_campaign_results;
CREATE TABLE default.HA_marketing_campaign_results
(
    campaign_id String,
    campaign_name LowCardinality(String),
    target_segment LowCardinality(String),
    start_date Date,
    end_date Date,
    total_clients UInt32,
    response_count UInt32,
    response_rate Float32,
    revenue_generated Decimal(15, 2),
    roi Float32,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(start_date)
ORDER BY (campaign_id, start_date)
SETTINGS index_granularity = 8192;

-- Таблица для дневных аналитических агрегатов
DROP TABLE IF EXISTS default.HA_marketing_analytics_daily;
CREATE TABLE default.HA_marketing_analytics_daily
(
    analysis_date Date,
    segment LowCardinality(String),
    clients_count UInt32,
    total_revenue Decimal(15, 2),
    avg_revenue Decimal(12, 2),
    active_clients UInt32,
    new_clients UInt32,
    created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(analysis_date)
ORDER BY (analysis_date, segment)
SETTINGS index_granularity = 8192;

-- Таблица для эффективности кампаний
DROP TABLE IF EXISTS default.HA_campaign_performance;
CREATE TABLE default.HA_campaign_performance
(
    campaign_id String,
    segment LowCardinality(String),
    impressions UInt32,
    clicks UInt32,
    conversions UInt32,
    ctr Float32,
    conversion_rate Float32,
    roi Float32,
    performance_category LowCardinality(String),
    analysis_date Date,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(analysis_date)
ORDER BY (campaign_id, analysis_date)
SETTINGS index_granularity = 8192;
