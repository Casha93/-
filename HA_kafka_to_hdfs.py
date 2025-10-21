#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, from_json
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, TimestampType, BooleanType, DateType
)

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
KAFKA_SERVERS = "172.17.0.13:9092"
HDFS_HOST = "hdfs://rc1a-dataproc-m-ucdvdhi2gxsxj4y9.mdb.yandexcloud.net:8020"
HDFS_RAW_PATH = f"{HDFS_HOST}/user/a.hryapin/ha_raw"

# Маппинг топиков Kafka в таблицы HDFS
TABLE_MAPPING = {
    "HA_clients": "ha_clients_raw",
    "HA_bank_transactions": "ha_bank_transactions_raw",
    "HA_client_logins": "ha_client_logins_raw",
    "HA_client_activities": "ha_client_activities_raw",
    "HA_payments": "ha_payments_raw",
    "HA_securities_portfolios": "ha_portfolios_raw",
    "HA_currency_rates": "ha_currency_rates_raw"
}

# Схемы JSON для парсинга
SCHEMAS = {
    "HA_clients": StructType([
        StructField("client_id", StringType(), False),
        StructField("client_name", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", DateType(), True),
        StructField("registration_date", DateType(), True),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("status", StringType(), True),
    ]),

    "HA_bank_transactions": StructType([
        StructField("transaction_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("channel", StringType(), True),
    ]),

    "HA_client_logins": StructType([
        StructField("login_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("login_timestamp", TimestampType(), False),
        StructField("ip_address", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("success", BooleanType(), True),
    ]),

    "HA_client_activities": StructType([
        StructField("activity_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("activity_date", DateType(), False),
        StructField("activity_type", StringType(), False),
        StructField("duration_minutes", IntegerType(), True),
        StructField("page_views", IntegerType(), True),
    ]),

    "HA_payments": StructType([
        StructField("payment_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("payment_date", DateType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("payment_type", StringType(), False),
        StructField("status", StringType(), True),
    ]),

    "HA_securities_portfolios": StructType([
        StructField("portfolio_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("security_type", StringType(), False),
        StructField("security_name", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("purchase_price", DoubleType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("purchase_date", DateType(), False),
    ]),

    "HA_currency_rates": StructType([
        StructField("rate_id", StringType(), False),
        StructField("currency_pair", StringType(), False),
        StructField("rate_date", TimestampType(), False),
        StructField("rate_value", DoubleType(), False),
        StructField("rate_type", StringType(), True),
    ])
}

# Создание Spark-сессии (без Hive)
def get_spark_session():
    logger.info("Создание Spark-сессии (без Hive)...")
    return SparkSession.builder \
        .appName("HA_Kafka_to_HDFS_Raw") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

# Чтение данных из топика Kafka
def read_kafka_topic(spark, topic):
    logger.info(f"Чтение из Kafka топика: {topic}")
    return spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

# Обработка одного топика
def process_topic(spark, topic, table_name, processing_date):
    try:
        logger.info("=" * 70)
        logger.info(f"Обработка топика: {topic} -> {table_name}")

        kafka_df = read_kafka_topic(spark, topic)
        value_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

        schema = SCHEMAS.get(topic)
        if not schema:
            logger.error(f"Нет схемы для топика {topic}")
            return 0

        json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

        final_df = json_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("processing_date", lit(processing_date))

        output_path = f"{HDFS_RAW_PATH}/{processing_date}/{table_name}"

        logger.info(f"Запись в HDFS: {output_path}")
        final_df.write.mode("overwrite").parquet(output_path)

        count = final_df.count()
        logger.info(f"Успешно обработано записей: {count}")
        logger.info(f"Сохранено: {output_path}")
        return count

    except Exception as e:
        logger.error(f"Ошибка при обработке топика {topic}: {str(e)}")
        logger.error("Детали:", exc_info=True)
        return 0

# Точка входа
def main():
    if len(sys.argv) != 3:
        logger.error("Использование: HA_kafka_to_hdfs.py <processing_date> '<topic1> <topic2> ...'")
        logger.error("Пример: HA_kafka_to_hdfs.py 2025-10-13 'HA_clients HA_bank_transactions'")
        sys.exit(1)

    execution_date = sys.argv[1]

    # Поддержка списка топиков через пробелы и запятые
    topics_raw = sys.argv[2]
    topics = topics_raw.replace(',', ' ').split()
    topics = [t.strip() for t in topics if t.strip()]

    logger.info("=" * 70)
    logger.info("СТАРТ: Kafka → HDFS (ШАГ 1)")
    logger.info("=" * 70)
    logger.info(f"Дата обработки: {execution_date}")
    logger.info(f"Кол-во топиков: {len(topics)}")
    logger.info(f"Топики: {', '.join(topics)}")
    logger.info(f"HDFS RAW: {HDFS_RAW_PATH}")
    logger.info("=" * 70)

    start_time = datetime.now()

    try:
        spark = get_spark_session()

        total_records = 0
        successful_topics = 0
        failed_topics = 0

        for topic in topics:
            if topic not in TABLE_MAPPING:
                logger.warning(f"Топик {topic} отсутствует в TABLE_MAPPING, пропуск")
                continue

            table_name = TABLE_MAPPING[topic]
            count = process_topic(spark, topic, table_name, execution_date)

            if count > 0:
                total_records += count
                successful_topics += 1
            else:
                failed_topics += 1

        elapsed_seconds = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("ЗАВЕРШЕНИЕ: Kafka → HDFS (ШАГ 1)")
        logger.info("=" * 70)
        logger.info(f"Успешных топиков: {successful_topics}")
        logger.info(f"Проваленных топиков: {failed_topics}")
        logger.info(f"Всего записей: {total_records}")
        logger.info(f"Время выполнения: {elapsed_seconds:.1f}s")
        logger.info(f"Каталог RAW: {HDFS_RAW_PATH}")
        logger.info("=" * 70)

        spark.stop()
        sys.exit(0 if failed_topics == 0 else 1)

    except Exception as e:
        logger.error("=" * 70)
        logger.error("КРИТИЧЕСКАЯ ОШИБКА")
        logger.error("=" * 70)
        logger.error(f"Ошибка: {str(e)}")
        logger.error("Стек:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
