# -*- coding: utf-8 -*-

from __future__ import annotations
import sys
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Пути и импорты утилит пайплайна
sys.path.append('/home/a.hryapin/final_project')
from HA_pipeline_utils import (
    validate_generated_data,
    monitor_pipeline_performance,
    on_success_callback,
    on_failure_callback
)

# Базовые настройки путей и коннекшенов
CLUSTER_PROJECT_PATH = "/home/a.hryapin/final_project"
DATA_PATH = f"{CLUSTER_PROJECT_PATH}/data"
SSH_CONNECTION_ID = "hryapin_ssh"
GREENPLUM_CONN_ID = "hryapin_gp_con"

# Темы Kafka (через пробел)
KAFKA_TOPICS = " ".join([
    "HA_clients",
    "HA_bank_transactions",
    "HA_client_logins",
    "HA_client_activities",
    "HA_payments",
    "HA_securities_portfolios",
    "HA_currency_rates"
])

# Конфиг Spark для задач Kafka→HDFS
SPARK_KAFKA_TO_HDFS = """spark-submit \\
--master yarn --deploy-mode client \\
--executor-memory 2g --executor-cores 1 --num-executors 1 --driver-memory 1g \\
--conf spark.sql.shuffle.partitions=10 \\
--conf spark.sql.adaptive.enabled=false \\
--conf spark.dynamicAllocation.enabled=false \\
--conf spark.yarn.executor.memoryOverhead=512m \\
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3"""

# Конфиг Spark для очистки HDFS
SPARK_HDFS_CLEANING = """spark-submit \\
--master yarn --deploy-mode client \\
--executor-memory 2g --executor-cores 1 --num-executors 1 --driver-memory 1g \\
--conf spark.sql.shuffle.partitions=10 \\
--conf spark.sql.adaptive.enabled=false \\
--conf spark.dynamicAllocation.enabled=false \\
--conf spark.yarn.executor.memoryOverhead=512m"""

# Параметры по умолчанию для DAG
DEFAULT_ARGS = {
    'owner': 'ha_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'sla': timedelta(hours=3),
}

with DAG(
    dag_id="HA_DataPipeline_TwoStep_v1",
    start_date=pendulum.datetime(2025, 10, 13, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=2),
    template_searchpath=['/opt/airflow/dags/wave26_HA_sql/'],
    tags=["HA", "kafka", "hdfs", "hive", "greenplum", "clickhouse", "two-step"],
    default_args=DEFAULT_ARGS
) as dag:

    # Подготовка окружения
    with TaskGroup("preparation") as prep_group:
        create_directories = SSHOperator(
            task_id="create_directories",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=60,
            command=f"mkdir -p {DATA_PATH}/{{{{ data_interval_end | ds }}}}"
        )

        check_scripts = SSHOperator(
            task_id="check_scripts",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=30,
            command=f"ls -la {CLUSTER_PROJECT_PATH}/HA_*.py"
        )

        cleanup_derby = SSHOperator(
            task_id="cleanup_derby",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=30,
            command="""
                rm -rf /var/lib/hive/metastore/metastore_db 2>/dev/null || true
                rm -rf ~/metastore_db 2>/dev/null || true
                rm -rf /home/a.hryapin/metastore_db 2>/dev/null || true
                find /home/a.hryapin -name "derby.log" -delete 2>/dev/null || true
            """
        )

        create_directories >> check_scripts >> cleanup_derby

    # Генерация и валидация данных
    with TaskGroup("data_generation") as data_gen_group:
        generate_data = SSHOperator(
            task_id="generate_data",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=300,
            command=f"""
                cd {CLUSTER_PROJECT_PATH} && \
                pip3 install --user numpy pandas faker --quiet && \
                python3 HA_project_generator.py {DATA_PATH}/{{{{ data_interval_end | ds }}}}
            """
        )

        validate_data = PythonOperator(
            task_id="validate_data",
            python_callable=validate_generated_data
        )

        generate_data >> validate_data

    # Поток в Kafka
    with TaskGroup("kafka_streaming") as kafka_group:
        kafka_producer = SSHOperator(
            task_id="kafka_producer",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=600,
            command=f"""
                cd {CLUSTER_PROJECT_PATH} && \
                pip3 install --user kafka-python --quiet && \
                python3 HA_project_kafka_producer.py {DATA_PATH}/{{{{ data_interval_end | ds }}}}
            """
        )

    # Обработка Spark: Kafka→HDFS RAW и очистка HDFS
    with TaskGroup("spark_processing") as spark_group:
        kafka_to_hdfs = SSHOperator(
            task_id="kafka_to_hdfs_raw",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=1800,
            command=f"""
                cd {CLUSTER_PROJECT_PATH} && \
                {SPARK_KAFKA_TO_HDFS} \
                HA_kafka_to_hdfs.py {{{{ data_interval_end | ds }}}} "{KAFKA_TOPICS}"
            """
        )

        hdfs_cleaning = SSHOperator(
            task_id="hdfs_cleaning",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=900,
            command=f"""
                cd {CLUSTER_PROJECT_PATH} && \
                {SPARK_HDFS_CLEANING} \
                HA_hdfs_to_hive_cleaned.py {{{{ data_interval_end | ds }}}}
            """
        )

        update_hive_partitions = SSHOperator(
            task_id="update_hive_partitions",
            ssh_conn_id=SSH_CONNECTION_ID,
            cmd_timeout=180,
            command=f"""
                cd {CLUSTER_PROJECT_PATH} && \
                python3 HA_add_hive_partitions.py {{{{ data_interval_end | ds }}}}
            """
        )

        kafka_to_hdfs >> hdfs_cleaning >> update_hive_partitions

    # Greenplum: внешние таблицы и сегментация
    with TaskGroup("greenplum_processing") as gp_group:
        create_gp_tables = PostgresOperator(
            task_id="create_gp_tables",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="HA_create_gp_raw_external_tables.sql",
            autocommit=True
        )

        gp_segmentation = PostgresOperator(
            task_id="gp_segmentation",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="HA_greenplum_segmentation.sql",
            autocommit=True
        )

        create_gp_tables >> gp_segmentation

    # ClickHouse: создание таблиц
    create_ch_tables = SSHOperator(
        task_id="create_ch_tables",
        ssh_conn_id=SSH_CONNECTION_ID,
        cmd_timeout=180,
        command=f"""
            cd {CLUSTER_PROJECT_PATH} && \
            pip3 install --user clickhouse-connect --quiet && \
            python3 << 'PYEOF'
import clickhouse_connect
client = clickhouse_connect.get_client(host='172.17.1.18', port=8123, username='default', password='')
with open('HA_clickhouse_create_tables.sql', 'r') as f:
    sql = f.read()
for statement in sql.split(';'):
    s = statement.strip()
    if s and not s.startswith('--'):
        try:
            client.command(s)
            print(f'Executed: {{s[:60]}}...')
        except Exception as e:
            print(f'Error: {{e}}')
print('Tables created successfully')
client.close()
PYEOF
        """
    )

    # Генерация маркетинговых предложений
    marketing_offers = SSHOperator(
        task_id="marketing_offers",
        ssh_conn_id=SSH_CONNECTION_ID,
        cmd_timeout=600,
        command=f"""
            cd {CLUSTER_PROJECT_PATH} && \
            pip3 install --user psycopg2-binary clickhouse-connect --quiet && \
            python3 HA_marketing_offers_generator.py {{{{ data_interval_end | ds }}}}
        """
    )

    # Запуск аналитики в ClickHouse
    run_ch_analytics = SSHOperator(
        task_id="run_ch_analytics",
        ssh_conn_id=SSH_CONNECTION_ID,
        cmd_timeout=300,
        command=f"""
            cd {CLUSTER_PROJECT_PATH} && \
            pip3 install --user clickhouse-connect --quiet && \
            python3 << 'PYEOF'
import clickhouse_connect
client = clickhouse_connect.get_client(host='172.17.1.18', port=8123, username='default', password='')
with open('HA_clickhouse_marketing_analytics.sql', 'r') as f:
    sql = f.read()
for statement in sql.split(';'):
    s = statement.strip()
    if s and not s.startswith('--'):
        try:
            client.command(s)
            print(f'Executed: {{s[:60]}}...')
        except Exception as e:
            print(f'Warning: {{e}}')
print('Analytics completed successfully')
client.close()
PYEOF
        """
    )

    # Мониторинг и финальные статусы
    monitor_performance = PythonOperator(
        task_id="monitor_performance",
        python_callable=monitor_pipeline_performance,
        trigger_rule=TriggerRule.ALL_DONE
    )

    pipeline_success = DummyOperator(
        task_id="pipeline_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=on_success_callback
    )

    pipeline_failure = DummyOperator(
        task_id="pipeline_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
        on_failure_callback=on_failure_callback
    )

    # Граф зависимостей
    prep_group >> data_gen_group >> kafka_group >> spark_group >> gp_group
    gp_group >> create_ch_tables
    create_ch_tables >> marketing_offers
    marketing_offers >> run_ch_analytics
    run_ch_analytics >> monitor_performance
    monitor_performance >> [pipeline_success, pipeline_failure]
