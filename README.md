# Электронный брокер консультант

Проект решает задачу сегментации банковских клиентов и генерации таргетированных маркетинговых предложений, чтобы повышать прибыльность банка и ценность портфелей ценных бумаг клиентов за счет персонализации и аналитики в реальном времени.​

Назначение и цели
Сегментация клиентов по размерам портфелей и поведенческой активности транзакций для приоритизации «next best action» и маркетинговых кампаний.​

Построение надежного дата-пайплайна от Kafka до ClickHouse/Greenplum с очисткой и обогащением данных, пригодного для регулярного продакшн-прогона и визуальной аналитики.​

Архитектура
Источники: генератор синтетических данных + поток из Kafka; хранение в HDFS (raw/cleaned), структуризация в Hive-подобном формате.​

Обработка: Spark джобы для загрузки из Kafka в HDFS и последующей очистки/нормализации данных.​

Хранилище: Greenplum для агрегатов и правил сегментации, внешние таблицы через PXF.​

Витрины и аналитика: ClickHouse таблицы клиентов/офферов и ежедневная маркетинговая аналитика.​

Оркестрация: один DAG в Airflow для end‑to‑end прогона (генерация→Kafka→Spark→Greenplum→ClickHouse→аналитика).​

Основные компоненты
DAG: HA_hryapin_project_DataPipeLine_airflow.py — таск-группы подготовки, генерации, стриминга, Spark-обработки, загрузки в Greenplum и ClickHouse, аналитика и мониторинг.​

Поток/обработка: HA_project_kafka_producer.py, HA_kafka_to_hdfs.py, HA_hdfs_to_hive_cleaned.py.​

Хранилище/SQL: HA_create_gp_raw_external_tables.sql, HA_greenplum_segmentation.sql.​

Витрины/аналитика: HA_clickhouse_create_tables.sql, HA_clickhouse_marketing_analytics.sql, HA_marketing_offers_generator.py.​

Данные: HA_project_generator.py — создание CSV для локальных прогонов и тестов.​

Быстрый старт
Подготовить инфраструктуру: Kafka, HDFS, Spark on YARN, Greenplum (PXF), ClickHouse, Airflow; задать подключения SSH и PostgreSQL в оркестраторе.​

Сгенерировать данные: python HA_project_generator.py /path/to/output и запустить продюсер Kafka по всем топикам.​

Прогнать DAG: секции Kafka→HDFS→Cleaned→Greenplum→ClickHouse→аналитика; DAG включает spark-submit команды и SQL/скрипты.​

Проверить витрины и отчеты: ClickHouse таблицы HAmarketingclients, HAtargetedoffers; выполнить ежедневные агрегаты аналитики.​

Сегментация и офферы
Сегментация вычисляется в Greenplum: агрегаты транзакций/логинов/активностей/портфелей и присвоение сегмента (VIP/Premium/Standard/Basic) с метриками и датой обработки.​

Генерация офферов: маппинг шаблонов по комбинированному сегменту, оценка ожидаемой выручки, запись клиентов и офферов в ClickHouse.​

Переменные/подключения
Greenplum: conn_id hryapin_gp_con или переменные окружения GPHOST/GPUSER/GPPASSWORD и т.д.​

ClickHouse: host/port/username/password задаются в конфиге скриптов и в задачах DAG.​

SSH/пути к кластеру: CLUSTER_PROJECT_PATH и DATA_PATH в DAG, параметры spark-submit для Kafka→HDFS и cleaning.​

Команды для локального прогона
Генерация CSV: python HA_project_generator.py ./data_out.​

Kafka продюсер: python HA_project_kafka_producer.py ./data_out (batched, сжатие lz4, параллельная отправка).​

Spark загрузка: spark-submit ... HA_kafka_to_hdfs.py <processing_date> <topics>.​

Spark очистка: spark-submit ... HA_hdfs_to_hive_cleaned.py <processing_date>.​

Результаты и артефакты
Greenplum: таблица haclientsegmentation с агрегатами и текущим сегментом клиента.​

ClickHouse: HAmarketingclients и HAtargetedoffers для витрин и BI; ежедневный срез HAmarketinganalyticsdaily.​

<img width="1388" height="320" alt="image" src="https://github.com/user-attachments/assets/e2c96a19-f66d-43c1-aee0-32114786b8b7" />

