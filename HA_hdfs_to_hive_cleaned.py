#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, regexp_replace, col, lit

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация путей в HDFS
HDFS_HOST = "hdfs://rc1a-dataproc-m-ucdvdhi2gxsxj4y9.mdb.yandexcloud.net:8020"
HDFS_RAW_PATH = f"{HDFS_HOST}/user/a.hryapin/ha_raw"
HDFS_CLEANED_PATH = f"{HDFS_HOST}/user/a.hryapin/ha_cleaned"

# Список сырых таблиц
RAW_TABLES = [
    "ha_clients_raw",
    "ha_bank_transactions_raw",
    "ha_client_logins_raw",
    "ha_client_activities_raw",
    "ha_payments_raw",
    "ha_portfolios_raw",
    "ha_currency_rates_raw"
]

def get_spark_session():
    # Сессия Spark без поддержки Hive
    logger.info("Создание Spark-сессии (без Hive)...")
    return SparkSession.builder \
        .appName("HA_HDFS_Cleaning") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

def clean_data(df, table_name):
    # Базовые очистки по типам таблиц
    logger.info(f"Применение правил качества для {table_name}")

    if "clients" in table_name:
        if "phone" in df.columns:
            df = df.withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
        if "client_name" in df.columns:
            df = df.withColumn("client_name", trim(col("client_name")))
        if "email" in df.columns:
            df = df.withColumn("email", trim(col("email")))

    elif "transactions" in table_name:
        if "transaction_type" in df.columns:
            df = df.withColumn("transaction_type", upper(trim(col("transaction_type"))))
        if "currency" in df.columns:
            df = df.withColumn("currency", upper(trim(col("currency"))))

    elif "portfolios" in table_name:
        if "security_type" in df.columns:
            df = df.withColumn("security_type", upper(trim(col("security_type"))))

    elif "currency_rates" in table_name:
        if "currency_pair" in df.columns:
            df = df.withColumn("currency_pair", upper(trim(col("currency_pair"))))

    elif "payments" in table_name:
        if "payment_type" in df.columns:
            df = df.withColumn("payment_type", upper(trim(col("payment_type"))))
        if "currency" in df.columns:
            df = df.withColumn("currency", upper(trim(col("currency"))))

    elif "activities" in table_name:
        if "activity_type" in df.columns:
            df = df.withColumn("activity_type", upper(trim(col("activity_type"))))

    elif "logins" in table_name:
        if "device_type" in df.columns:
            df = df.withColumn("device_type", upper(trim(col("device_type"))))

    return df

def process_table(spark, raw_table, processing_date):
    # Обработка одной таблицы: чтение RAW -> очистка -> запись в CLEANED
    cleaned_table = raw_table.replace("_raw", "_structured")

    try:
        logger.info("=" * 70)
        logger.info(f"Обработка: {raw_table} -> {cleaned_table}")

        raw_path = f"{HDFS_RAW_PATH}/{processing_date}/{raw_table}"
        logger.info(f"Чтение из HDFS: {raw_path}")
        df = spark.read.parquet(raw_path)

        logger.info("Схема до очистки:")
        df.printSchema()

        cleaned_df = clean_data(df, raw_table)

        # Гарантируем наличие колонки разделения
        if "processing_date" not in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn("processing_date", lit(processing_date))

        # Удаляем служебные поля, не нужные в cleaned
        if "ingestion_timestamp" in cleaned_df.columns:
            cleaned_df = cleaned_df.drop("ingestion_timestamp")

        cleaned_path = f"{HDFS_CLEANED_PATH}/{cleaned_table}"
        logger.info(f"Запись в HDFS: {cleaned_path}")

        cleaned_df.write \
            .mode("overwrite") \
            .partitionBy("processing_date") \
            .parquet(cleaned_path)

        count = cleaned_df.count()
        logger.info(f"Успешно обработано записей: {count}")
        logger.info(f"Данные сохранены: {cleaned_path}")
        return count

    except Exception as e:
        logger.error(f"Ошибка при обработке {raw_table}: {str(e)}")
        logger.error("Детали:", exc_info=True)
        logger.warning(f"RAW остаётся без изменений: {HDFS_RAW_PATH}/{raw_table}")
        return 0

def main():
    # Точка входа
    if len(sys.argv) != 2:
        logger.error("Использование: HA_hdfs_to_hive_cleaned.py <processing_date>")
        logger.error("Пример: HA_hdfs_to_hive_cleaned.py 2025-10-13")
        sys.exit(1)

    execution_date = sys.argv[1]

    logger.info("=" * 70)
    logger.info("СТАРТ: Очистка данных в HDFS (ШАГ 2)")
    logger.info("=" * 70)
    logger.info(f"Дата обработки: {execution_date}")
    logger.info(f"Кол-во таблиц: {len(RAW_TABLES)}")
    logger.info(f"HDFS RAW: {HDFS_RAW_PATH}")
    logger.info(f"HDFS CLEANED: {HDFS_CLEANED_PATH}")
    logger.info("=" * 70)

    start_time = datetime.now()

    try:
        spark = get_spark_session()

        total_records = 0
        successful_tables = 0
        failed_tables = 0

        for raw_table in RAW_TABLES:
            count = process_table(spark, raw_table, execution_date)
            if count > 0:
                total_records += count
                successful_tables += 1
            else:
                failed_tables += 1

        elapsed_seconds = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("ЗАВЕРШЕНИЕ: Очистка данных в HDFS (ШАГ 2)")
        logger.info("=" * 70)
        logger.info(f"Успешных таблиц: {successful_tables}")
        logger.info(f"Проваленных таблиц: {failed_tables}")
        logger.info(f"Всего записей: {total_records}")
        logger.info(f"Время выполнения: {elapsed_seconds:.1f}s")
        logger.info(f"Каталог CLEANED: {HDFS_CLEANED_PATH}")
        logger.info("=" * 70)

        if failed_tables > 0:
            logger.warning("Есть ошибки в части таблиц, RAW-данные сохранены.")
            logger.warning("ШАГ 2 можно перезапустить отдельно после фикса.")

        logger.info("СЛЕДУЮЩИЙ ШАГ: создать внешние таблицы Hive через hive CLI")
        logger.info("Команда: python3 /home/a.hryapin/final_project/HA_create_hive_tables.py")
        logger.info("=" * 70)

        spark.stop()
        sys.exit(0 if failed_tables == 0 else 1)

    except Exception as e:
        logger.error("=" * 70)
        logger.error("КРИТИЧЕСКАЯ ОШИБКА В ШАГЕ 2")
        logger.error("=" * 70)
        logger.error(f"Ошибка: {str(e)}")
        logger.error("Стек:", exc_info=True)
        logger.error("=" * 70)
        logger.error("RAW-данные сохранены в HDFS")
        logger.error(f"Путь RAW: {HDFS_RAW_PATH}")
        logger.error("Перезапустите ШАГ 2 после исправления")
        logger.error("=" * 70)
        sys.exit(1)

if __name__ == "__main__":
    main()
