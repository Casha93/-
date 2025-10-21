import os
import sys
import time
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    # Конфигурация Kafka producer
    bootstrap_servers: str = "172.17.0.13:9092"
    batch_size: int = 16384
    linger_ms: int = 5
    buffer_memory: int = 33554432
    compression_type: str = 'lz4'
    acks: int = 1
    retries: int = 3
    max_in_flight: int = 5
    request_timeout_ms: int = 30000
    # Параметры производительности
    chunk_size: int = 2000
    max_workers: int = 2

# Маппинг файлов в Kafka-топики
TOPIC_MAPPING = {
    "clients.csv": "HA_clients",
    "bank_transactions.csv": "HA_bank_transactions",
    "client_logins.csv": "HA_client_logins",
    "client_activities.csv": "HA_client_activities",
    "payments.csv": "HA_payments",
    "securities_portfolios.csv": "HA_securities_portfolios",
    "currency_rates.csv": "HA_currency_rates"
}

class OptimizedKafkaProducer:
    # Producer с батчингом и метриками
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = self._create_producer()
        self.metrics = {'total_sent': 0, 'total_errors': 0, 'files_processed': 0, 'start_time': time.time()}

    def _create_producer(self) -> KafkaProducer:
        # Создание Kafka producer
        cfg = {
            'bootstrap_servers': self.config.bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            'batch_size': self.config.batch_size,
            'linger_ms': self.config.linger_ms,
            'buffer_memory': self.config.buffer_memory,
            'compression_type': self.config.compression_type,
            'acks': self.config.acks,
            'retries': self.config.retries,
            'retry_backoff_ms': 300,
            'max_in_flight_requests_per_connection': self.config.max_in_flight,
            'request_timeout_ms': self.config.request_timeout_ms,
            'connections_max_idle_ms': 300000,
        }
        try:
            producer = KafkaProducer(**cfg)
            logger.info(f"Kafka Producer создан. Сервер: {self.config.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Ошибка создания Kafka Producer: {e}")
            sys.exit(1)

    def send_file_optimized(self, topic: str, file_path: Path) -> Dict[str, int]:
        # Отправка файла батчами
        if not file_path.exists():
            logger.warning(f"Файл не найден: {file_path}")
            return {'sent': 0, 'errors': 0}

        logger.info(f"Обработка файла: {file_path} -> {topic}")
        start = time.time()

        try:
            df = pd.read_parquet(file_path) if file_path.suffix.lower() == '.parquet' else pd.read_csv(file_path)
            total = len(df)
            logger.info(f"Загружено {total:,} записей из {file_path.name}")

            result = self._send_dataframe_batched(df, topic)

            sec = time.time() - start
            tps = total / sec if sec > 0 else 0
            logger.info(f"{file_path.name} за {sec:.2f}с | отправлено {result['sent']:,}, ошибок {result['errors']:,} | {tps:.0f} rec/s")

            self.metrics['files_processed'] += 1
            self.metrics['total_sent'] += result['sent']
            self.metrics['total_errors'] += result['errors']
            return result
        except Exception as e:
            logger.error(f"Ошибка обработки {file_path}: {e}")
            self.metrics['total_errors'] += 1
            return {'sent': 0, 'errors': 1}

    def _send_dataframe_batched(self, df: pd.DataFrame, topic: str) -> Dict[str, int]:
        # Отправка DataFrame батчами с параллелизмом
        total = len(df)
        sent = 0
        errors = 0

        chunks = [df[i:i + self.config.chunk_size] for i in range(0, total, self.config.chunk_size)]
        logger.info(f"Батчей: {len(chunks)} по {self.config.chunk_size}")

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self._send_chunk_async, chunk, topic): i for i, chunk in enumerate(chunks)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    res = future.result()
                    sent += res['sent']
                    errors += res['errors']
                    if (idx + 1) % 10 == 0:
                        logger.info(f"Обработано батчей: {idx + 1}/{len(chunks)}")
                except Exception as e:
                    logger.error(f"Ошибка батча {idx}: {e}")
                    errors += len(chunks[idx])

        self.producer.flush()
        return {'sent': sent, 'errors': errors}

    def _send_chunk_async(self, chunk: pd.DataFrame, topic: str) -> Dict[str, int]:
        # Асинхронная отправка чанка
        sent = 0
        errors = 0
        futures = []
        records = chunk.to_dict('records')

        for rec in records:
            try:
                futures.append(self.producer.send(topic, value=rec))
            except Exception as e:
                logger.error(f"Ошибка отправки записи: {e}")
                errors += 1

        for f in futures:
            try:
                f.get(timeout=10)
                sent += 1
            except KafkaError as e:
                logger.error(f"Kafka ошибка: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"Ошибка отправки: {e}")
                errors += 1

        return {'sent': sent, 'errors': errors}

    def get_performance_metrics(self) -> Dict[str, float]:
        # Метрики производительности
        total_time = time.time() - self.metrics['start_time']
        return {
            'total_sent': self.metrics['total_sent'],
            'total_errors': self.metrics['total_errors'],
            'files_processed': self.metrics['files_processed'],
            'total_time_seconds': total_time,
            'throughput_records_per_sec': self.metrics['total_sent'] / total_time if total_time > 0 else 0,
            'error_rate_percent': (self.metrics['total_errors'] / max(1, self.metrics['total_sent'] + self.metrics['total_errors'])) * 100
        }

    def close(self):
        # Корректное закрытие producer
        logger.info("Закрытие Kafka Producer...")
        try:
            self.producer.flush()
            self.producer.close(timeout=30)
            logger.info("Kafka Producer закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии: {e}")

class OptimizedDataPipeline:
    # Пайплайн отправки данных в Kafka
    def __init__(self, data_directory: str, config: Optional[KafkaConfig] = None):
        self.data_directory = Path(data_directory)
        self.config = config or KafkaConfig()
        if not self.data_directory.exists():
            raise FileNotFoundError(f"Директория не существует: {data_directory}")
        logger.info(f"Инициализация пайплайна: {self.data_directory}")

    def run(self) -> Dict[str, Dict[str, int]]:
        # Запуск пайплайна
        producer = OptimizedKafkaProducer(self.config)
        results: Dict[str, Dict[str, int]] = {}
        try:
            logger.info("Старт отправки данных в Kafka...")
            with ThreadPoolExecutor(max_workers=len(TOPIC_MAPPING)) as executor:
                futures = {}
                for filename, topic in TOPIC_MAPPING.items():
                    file_path = self.data_directory / filename
                    futures[executor.submit(producer.send_file_optimized, topic, file_path)] = (filename, topic)

                for future in as_completed(futures):
                    filename, topic = futures[future]
                    try:
                        results[filename] = future.result()
                    except Exception as e:
                        logger.error(f"Ошибка {filename}: {e}")
                        results[filename] = {'sent': 0, 'errors': 1}

            self._log_final_metrics(producer, results)
        finally:
            producer.close()
        return results

    def _log_final_metrics(self, producer: OptimizedKafkaProducer, results: Dict[str, Dict[str, int]]):
        # Итоговые метрики
        m = producer.get_performance_metrics()
        logger.info("=" * 60)
        logger.info("ИТОГОВЫЕ МЕТРИКИ")
        logger.info("=" * 60)
        logger.info(f"Время: {m['total_time_seconds']:.2f} с")
        logger.info(f"Отправлено: {m['total_sent']:,}")
        logger.info(f"Ошибок: {m['total_errors']:,}")
        logger.info(f"Файлов: {m['files_processed']}")
        logger.info(f"Скорость: {m['throughput_records_per_sec']:.0f} записей/с")
        logger.info(f"Ошибка, %: {m['error_rate_percent']:.2f}")
        logger.info("\nРезультаты по файлам:")
        for filename, r in results.items():
            logger.info(f"  {filename}: отправлено {r['sent']:,}, ошибок {r['errors']:,}")

def main():
    # Точка входа
    if len(sys.argv) < 2:
        logger.error("Использование: python HA_project_kafka_producer.py <путь_к_данным>")
        sys.exit(1)

    data_directory = sys.argv[1]
    logger.info(f"Директория данных: {data_directory}")

    # Конфигурация производительности (демо-значения)
    config = KafkaConfig(
        batch_size=32768,
        linger_ms=10,
        chunk_size=5000,
        max_workers=4,
        compression_type='lz4'
    )

    try:
        pipeline = OptimizedDataPipeline(data_directory, config)
        pipeline.run()
        logger.info("Отправка данных завершена успешно")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
