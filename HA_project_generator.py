#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

def main():
    # Проверка аргументов
    if len(sys.argv) != 2:
        print("Использование: python3 HA_project_generator_simple.py <output_directory>")
        sys.exit(1)

    output_dir = sys.argv[1]

    # Создание каталога вывода
    os.makedirs(output_dir, exist_ok=True)
    print(f"Генерация данных в {output_dir}")

    # Настройки генерации (учебный кластер)
    fake = Faker('ru_RU')

    # Объемы данных
    num_clients = 5000
    num_transactions = 15000
    num_logins = 8000
    num_activities = 10000
    num_payments = 7000
    num_portfolios = 3000
    num_currency_rates = 1000

    print(f"Генерация {num_clients} клиентов...")

    # 1. Клиенты
    clients_data = []
    for i in range(num_clients):
        client_id = f"CLIENT_{i + 1:06d}"
        clients_data.append({
            'client_id': client_id,
            'client_name': fake.name(),
            'phone': fake.phone_number(),
            'email': fake.email(),
            'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'registration_date': fake.date_between(start_date='-5y', end_date='today'),
            'city': fake.city(),
            'segment': random.choice(['VIP', 'Premium', 'Standard', 'Basic']),
            'status': random.choice(['Active', 'Inactive'])
        })
    clients_df = pd.DataFrame(clients_data)
    clients_df.to_csv(os.path.join(output_dir, 'clients.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(clients_df)} клиентов")

    # 2. Банковские транзакции
    print(f"Генерация {num_transactions} транзакций...")
    client_ids = clients_df['client_id'].tolist()
    transactions_data = []
    for i in range(num_transactions):
        transactions_data.append({
            'transaction_id': f"TXN_{i + 1:08d}",
            'client_id': random.choice(client_ids),
            'transaction_date': fake.date_between(start_date='-1y', end_date='today'),
            'amount': round(random.uniform(100, 50000), 2),
            'currency': random.choice(['RUB', 'USD', 'EUR']),
            'transaction_type': random.choice(['Debit', 'Credit', 'Transfer']),
            'channel': random.choice(['ATM', 'Online', 'Branch', 'Mobile'])
        })
    transactions_df = pd.DataFrame(transactions_data)
    transactions_df.to_csv(os.path.join(output_dir, 'bank_transactions.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(transactions_df)} транзакций")

    # 3. Логины клиентов
    print(f"Генерация {num_logins} логинов...")
    logins_data = []
    for i in range(num_logins):
        logins_data.append({
            'login_id': f"LOGIN_{i + 1:08d}",
            'client_id': random.choice(client_ids),
            'login_timestamp': fake.date_time_between(start_date='-6m', end_date='now'),
            'ip_address': fake.ipv4(),
            'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'success': random.choice([True, False])
        })
    logins_df = pd.DataFrame(logins_data)
    logins_df.to_csv(os.path.join(output_dir, 'client_logins.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(logins_df)} логинов")

    # 4. Активности клиентов
    print(f"Генерация {num_activities} активностей...")
    activities_data = []
    for i in range(num_activities):
        activities_data.append({
            'activity_id': f"ACT_{i + 1:08d}",
            'client_id': random.choice(client_ids),
            'activity_date': fake.date_between(start_date='-3m', end_date='today'),
            'activity_type': random.choice(['Login', 'Transfer', 'Payment', 'View_Balance', 'Update_Profile']),
            'duration_minutes': random.randint(1, 30),
            'page_views': random.randint(1, 10)
        })
    activities_df = pd.DataFrame(activities_data)
    activities_df.to_csv(os.path.join(output_dir, 'client_activities.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(activities_df)} активностей")

    # 5. Платежи
    print(f"Генерация {num_payments} платежей...")
    payments_data = []
    for i in range(num_payments):
        payments_data.append({
            'payment_id': f"PAY_{i + 1:08d}",
            'client_id': random.choice(client_ids),
            'payment_date': fake.date_between(start_date='-6m', end_date='today'),
            'amount': round(random.uniform(50, 10000), 2),
            'currency': 'RUB',
            'payment_type': random.choice(['Utility', 'Mobile', 'Internet', 'Insurance']),
            'status': random.choice(['Completed', 'Pending', 'Failed'])
        })
    payments_df = pd.DataFrame(payments_data)
    payments_df.to_csv(os.path.join(output_dir, 'payments.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(payments_df)} платежей")

    # 6. Портфели ценных бумаг
    print(f"Генерация {num_portfolios} портфелей...")
    portfolios_data = []
    for i in range(num_portfolios):
        portfolios_data.append({
            'portfolio_id': f"PORT_{i + 1:06d}",
            'client_id': random.choice(client_ids),
            'security_type': random.choice(['Stock', 'Bond', 'ETF', 'Mutual_Fund']),
            'security_name': fake.company(),
            'quantity': random.randint(1, 1000),
            'purchase_price': round(random.uniform(10, 5000), 2),
            'current_price': round(random.uniform(10, 5000), 2),
            'purchase_date': fake.date_between(start_date='-2y', end_date='today')
        })
    portfolios_df = pd.DataFrame(portfolios_data)
    portfolios_df.to_csv(os.path.join(output_dir, 'securities_portfolios.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(portfolios_df)} портфелей")

    # 7. Курсы валют
    print(f"Генерация {num_currency_rates} курсов валют...")
    currency_rates_data = []
    base_date = datetime.now() - timedelta(days=365)
    for i in range(num_currency_rates):
        currency_rates_data.append({
            'rate_id': f"RATE_{i + 1:06d}",
            'currency_pair': random.choice(['USD/RUB', 'EUR/RUB', 'GBP/RUB']),
            'rate_date': base_date + timedelta(days=i % 365),
            'rate_value': round(random.uniform(60, 100), 4),
            'rate_type': 'Close'
        })
    currency_rates_df = pd.DataFrame(currency_rates_data)
    currency_rates_df.to_csv(os.path.join(output_dir, 'currency_rates.csv'), index=False, encoding='utf-8')
    print(f"Сохранено {len(currency_rates_df)} курсов валют")

    # Итоговая статистика
    total_records = (
        len(clients_df) + len(transactions_df) + len(logins_df) +
        len(activities_df) + len(payments_df) + len(portfolios_df) +
        len(currency_rates_df)
    )

    print("=" * 50)
    print("ГЕНЕРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 50)
    print(f"Всего записей: {total_records:,}")
    print(f"Файлы в: {output_dir}")
    print("Файлы:")
    print(f"  - clients.csv: {len(clients_df):,}")
    print(f"  - bank_transactions.csv: {len(transactions_df):,}")
    print(f"  - client_logins.csv: {len(logins_df):,}")
    print(f"  - client_activities.csv: {len(activities_df):,}")
    print(f"  - payments.csv: {len(payments_df):,}")
    print(f"  - securities_portfolios.csv: {len(portfolios_df):,}")
    print(f"  - currency_rates.csv: {len(currency_rates_df):,}")
    print("=" * 50)

if __name__ == '__main__':
    main()
