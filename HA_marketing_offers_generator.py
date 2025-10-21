#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import clickhouse_connect
from datetime import datetime
from psycopg2.extras import RealDictCursor

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Коннекторы
GREENPLUM_CONN_ID = "hryapin_gp_con"

# Настройки ClickHouse
CH_CONFIG = {
    'host': '172.17.1.18',
    'port': 8123,
    'username': 'default',
    'password': '',
    'database': 'default'
}

# Подключение к GreenPlum
def get_gp_connection():
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        conn = hook.get_conn()
        logger.info(f"Подключение к GreenPlum через {GREENPLUM_CONN_ID} установлено")
        return conn
    except ImportError:
        import psycopg2
        GP_CONFIG = {
            'host': os.getenv('GP_HOST', '172.17.1.32'),
            'port': int(os.getenv('GP_PORT', '5432')),
            'database': os.getenv('GP_DATABASE', 'wave26_team_a'),
            'user': os.getenv('GP_USER', 'wave26_user_a1'),
            'password': os.getenv('GP_PASSWORD', 'pass')
        }
        logger.warning("Airflow недоступен, используем прямое подключение к GP")
        return psycopg2.connect(**GP_CONFIG)
    except Exception as e:
        logger.error(f"Ошибка подключения к GP: {e}")
        raise

# Подключение к ClickHouse
def get_ch_connection():
    try:
        return clickhouse_connect.get_client(**CH_CONFIG)
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")
        raise

# Получение сегментов клиентов из GreenPlum
def get_client_segments_from_gp(gp_conn, target_date):
    segments_sql = """
    SELECT 
        client_id::TEXT AS client_id,
        client_name,
        segment AS portfolio_segment,
        segment AS transaction_segment,
        CASE 
            WHEN segment = 'VIP' AND login_count >= 5 THEN 'VIP Active'
            WHEN segment = 'VIP' THEN 'VIP Passive'
            WHEN segment = 'Premium' AND login_count >= 10 THEN 'Premium Active'
            WHEN segment = 'Premium' THEN 'Premium Passive'
            WHEN segment = 'Standard' AND login_count >= 5 THEN 'Standard Active'
            WHEN segment = 'Standard' THEN 'Standard Passive'
            WHEN transaction_count > 0 THEN 'Transaction Only'
            ELSE 'Low Engagement'
        END AS combined_segment,
        total_transaction_amount AS total_value,
        (login_count + activity_count) AS activity_score,
        CASE 
            WHEN segment = 'VIP' THEN 1
            WHEN segment = 'Premium' THEN 2
            WHEN segment = 'Standard' THEN 3
            ELSE 4
        END AS priority_level
    FROM hryapin.ha_client_segmentation
    WHERE processing_date = (
        SELECT MAX(processing_date) 
        FROM hryapin.ha_client_segmentation
    )
    ORDER BY total_transaction_amount DESC, login_count DESC
    """
    with gp_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(segments_sql)
        segments = cur.fetchall()
        logger.info(f"Получено {len(segments):,} сегментов из GreenPlum")
        return segments

# Генерация персональных предложений
def generate_marketing_offers(segments):
    offer_templates = {
        'VIP Active': {
            'offers': [
                ('Premium Investment Advisory', 'Персональный инвестиционный консультант', 50000, 1),
                ('Exclusive Private Banking', 'VIP банковское обслуживание', 30000, 1),
                ('Luxury Credit Card', 'Премиальная кредитная карта без лимита', 25000, 2)
            ]
        },
        'VIP Passive': {
            'offers': [
                ('Investment Portfolio Review', 'Анализ и оптимизация портфеля', 35000, 1),
                ('Premium Savings Account', 'Депозит под повышенный процент', 20000, 2),
                ('VIP Insurance Package', 'Комплексная страховая программа', 15000, 2)
            ]
        },
        'Premium Active': {
            'offers': [
                ('Advanced Trading Platform', 'Профессиональная торговая платформа', 20000, 2),
                ('Investment Consultation', 'Консультация по инвестициям', 15000, 2),
                ('Premium Credit Limit', 'Увеличенный кредитный лимит', 10000, 3)
            ]
        },
        'Premium Passive': {
            'offers': [
                ('Balanced Investment Fund', 'Сбалансированный инвестиционный фонд', 12000, 2),
                ('High-Yield Deposit', 'Высокодоходный депозит', 8000, 3),
                ('Premium Debit Card', 'Премиальная дебетовая карта', 5000, 3)
            ]
        },
        'Standard Active': {
            'offers': [
                ('Trading Mobile App', 'Мобильное приложение для торговли', 8000, 3),
                ('Investment Education', 'Обучающие курсы по инвестициям', 5000, 3),
                ('Cashback Credit Card', 'Кредитная карта с кэшбэком', 3000, 4)
            ]
        },
        'Standard Passive': {
            'offers': [
                ('Starter Investment Package', 'Стартовый инвестиционный пакет', 5000, 4),
                ('Savings Account Plus', 'Накопительный счет с бонусами', 3000, 4),
                ('Basic Insurance', 'Базовая страховая программа', 2000, 4)
            ]
        },
        'Transaction Only': {
            'offers': [
                ('Investment Starter Guide', 'Руководство по началу инвестирования', 3000, 4),
                ('No-Fee Account', 'Счет без комиссий', 2000, 4),
                ('Financial Planning Tool', 'Инструмент финансового планирования', 1000, 5)
            ]
        },
        'Low Engagement': {
            'offers': [
                ('Welcome Bonus', 'Приветственный бонус', 1000, 5),
                ('Basic Banking Package', 'Базовый банковский пакет', 500, 5),
                ('Mobile Banking App', 'Мобильное банковское приложение', 200, 5)
            ]
        }
    }

    marketing_offers = []
    for segment in segments:
        client_id = segment['client_id']
        client_name = segment['client_name']
        combined_segment = segment['combined_segment']
        total_value = float(segment['total_value'] or 0)
        priority_level = segment['priority_level']

        segment_offers = offer_templates.get(combined_segment, offer_templates['Low Engagement'])

        for offer_type, description, base_revenue, offer_priority in segment_offers['offers']:
            expected_revenue = base_revenue
            if total_value > 1_000_000:
                expected_revenue = int(base_revenue * 1.5)
            elif total_value > 500_000:
                expected_revenue = int(base_revenue * 1.2)
            elif total_value < 50_000:
                expected_revenue = int(base_revenue * 0.7)

            days_valid = 30 if priority_level <= 2 else 14 if priority_level <= 3 else 7

            marketing_offers.append({
                'client_id': client_id,
                'client_name': client_name,
                'portfolio_segment': segment['portfolio_segment'],
                'transaction_segment': segment['transaction_segment'],
                'combined_segment': combined_segment,
                'offer_type': offer_type,
                'offer_description': description,
                'expected_revenue': expected_revenue,
                'priority_level': priority_level,
                'offer_priority': offer_priority,
                'days_valid': days_valid,
                'total_portfolio_value': total_value
            })

    logger.info(f"Сгенерировано {len(marketing_offers):,} персональных предложений")
    return marketing_offers

# Создание/проверка таблиц ClickHouse
def create_ch_tables_if_not_exist(ch_client):
    try:
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS default.HA_marketing_clients
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
            SETTINGS index_granularity = 8192
        """)
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS default.HA_targeted_offers
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
            SETTINGS index_granularity = 8192
        """)
        logger.info("Таблицы ClickHouse созданы/проверены")
    except Exception as e:
        logger.error(f"Ошибка создания таблиц ClickHouse: {e}")
        raise

# Сохранение сегментов в ClickHouse
def save_segments_to_clickhouse(ch_client, segments):
    if not segments:
        logger.warning("Нет сегментов для сохранения")
        return
    try:
        data = [
            (
                seg['client_id'],
                seg['client_name'],
                seg['portfolio_segment'],
                seg['transaction_segment'],
                seg['combined_segment'],
                float(seg['total_value'] or 0),
                int(seg['activity_score'] or 0),
                int(seg['priority_level'])
            )
            for seg in segments
        ]
        ch_client.insert(
            'default.HA_marketing_clients',
            data,
            column_names=[
                'client_id', 'client_name', 'portfolio_segment',
                'transaction_segment', 'combined_segment', 'total_value',
                'activity_score', 'priority_level'
            ]
        )
        logger.info(f"Сохранено {len(data):,} сегментов в ClickHouse")
    except Exception as e:
        logger.error(f"Ошибка сохранения сегментов: {e}")
        raise

# Сохранение предложений в ClickHouse
def save_offers_to_clickhouse(ch_client, offers):
    if not offers:
        logger.warning("Нет предложений для сохранения")
        return
    try:
        import hashlib
        data = []
        for i, offer in enumerate(offers, start=1):
            offer_hash = hashlib.md5(f"{offer['client_id']}_{offer['offer_type']}_{i}".encode()).hexdigest()
            offer_id = int(offer_hash[:16], 16) % (10 ** 15)
            data.append((
                offer_id,
                offer['client_id'],
                offer['client_name'],
                offer['portfolio_segment'],
                offer['transaction_segment'],
                offer['combined_segment'],
                offer['offer_type'],
                offer['offer_description'],
                float(offer['expected_revenue']),
                int(offer['priority_level']),
                int(offer['offer_priority']),
                int(offer['days_valid']),
                float(offer['total_portfolio_value'])
            ))
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            ch_client.insert(
                'default.HA_targeted_offers',
                data[i:i + batch_size],
                column_names=[
                    'offer_id', 'client_id', 'client_name', 'portfolio_segment',
                    'transaction_segment', 'combined_segment', 'offer_type',
                    'offer_description', 'expected_revenue', 'priority_level',
                    'offer_priority', 'days_valid', 'total_portfolio_value'
                ]
            )
        logger.info(f"Сохранено {len(data):,} предложений в ClickHouse")
    except Exception as e:
        logger.error(f"Ошибка сохранения предложений: {e}")
        raise

# Итоговый отчёт в лог
def generate_summary_report(ch_client):
    try:
        segments_stats = ch_client.query("""
            SELECT 
                combined_segment,
                COUNT(*) AS clients_count,
                SUM(total_value) AS total_value,
                AVG(total_value) AS avg_value,
                AVG(activity_score) AS avg_activity
            FROM default.HA_marketing_clients
            GROUP BY combined_segment
            ORDER BY total_value DESC
        """)
        logger.info("\n" + "=" * 80)
        logger.info("СТАТИСТИКА ПО СЕГМЕНТАМ:")
        logger.info("=" * 80)
        for row in segments_stats.result_rows:
            logger.info(
                f"  {row[0]:25s} | Клиенты: {row[1]:5,} | "
                f"Общая сумма: {row[2]:12,.0f} | Средняя: {row[3]:10,.0f} | Активность: {row[4]:.1f}"
            )

        offers_stats = ch_client.query("""
            SELECT 
                offer_type,
                COUNT(*) AS offers_count,
                SUM(expected_revenue) AS total_expected_revenue,
                AVG(expected_revenue) AS avg_expected_revenue
            FROM default.HA_targeted_offers
            GROUP BY offer_type
            ORDER BY total_expected_revenue DESC
            LIMIT 10
        """)
        logger.info("\n" + "=" * 80)
        logger.info("ТОП-10 ТИПОВ ПРЕДЛОЖЕНИЙ:")
        logger.info("=" * 80)
        for row in offers_stats.result_rows:
            logger.info(
                f"  {row[0]:40s} | Кол-во: {row[1]:5,} | "
                f"Общий доход: {row[2]:12,.0f} | Средний: {row[3]:10,.0f}"
            )
        logger.info("=" * 80 + "\n")
    except Exception as e:
        logger.error(f"Ошибка генерации отчёта: {e}")

# Точка входа
def main():
    if len(sys.argv) != 2:
        logger.error("Использование: python3 HA_marketing_offers_generator.py <target_date>")
        logger.error("Пример: python3 HA_marketing_offers_generator.py 2025-10-13")
        sys.exit(1)

    target_date = sys.argv[1]
    logger.info(f"Генерация маркетинговых предложений ({target_date})")

    gp_conn = None
    ch_client = None

    try:
        gp_conn = get_gp_connection()
        ch_client = get_ch_connection()

        create_ch_tables_if_not_exist(ch_client)

        segments = get_client_segments_from_gp(gp_conn, target_date)
        if not segments:
            logger.warning(f"Нет сегментов для даты {target_date}")
            return

        save_segments_to_clickhouse(ch_client, segments)

        marketing_offers = generate_marketing_offers(segments)

        save_offers_to_clickhouse(ch_client, marketing_offers)

        generate_summary_report(ch_client)

        logger.info("✓ Генерация маркетинговых предложений завершена")
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        logger.error("Traceback:")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if gp_conn:
            try:
                gp_conn.close()
                logger.info("Подключение к GreenPlum закрыто")
            except:
                pass
        if ch_client:
            try:
                ch_client.close()
                logger.info("Подключение к ClickHouse закрыто")
            except:
                pass

if __name__ == "__main__":
    main()
