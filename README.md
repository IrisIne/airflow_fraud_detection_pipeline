# Airflow Fraud Detection Pipeline

Репозиторий содержит решение домашней работы 3: потоковый скоринг транзакций из Kafka и переобучение модели по расписанию в Apache Airflow.

## Архитектура

```text
CSV -> Python producer -> Kafka transactions -> Fraud detector -> Kafka scores -> PostgreSQL
                                                  ^
PostgreSQL labeled_transactions -> Airflow -> Model registry
                                      |
                                      -> Data Quality reports
```

Компоненты:

1. `transaction-producer` читает CSV и отправляет JSON-сообщения в топик `transactions`.
2. `fraud-detector` выполняет препроцессинг, применяет модель и записывает результат в `scores`.
3. `score-writer` сохраняет `transaction_id`, `score`, `fraud_flag` и `model_version` в PostgreSQL.
4. `maintain_labeled_data` добавляет следующий размеченный месяц и удаляет данные вне глубины хранения.
5. `retrain_fraud_model` формирует временной split, обучает модель и публикует прошедшую проверку версию.
6. `check_data_quality` проверяет витрину и сохраняет HTML.
7. Airflow и сервис скоринга используют общий каталог `shared/models`.

## Требования

- Docker 20.10 или новее;
- Docker Compose 2.0 или новее.

## Быстрый запуск

Скопируйте настройки окружения:

```bash
cp .env.example .env
```

Соберите и запустите сервисы:

```bash
docker compose up -d --build
docker compose ps -a
```

Контейнеры `airflow-permissions`, `airflow-init`, `pipeline-bootstrap` и `kafka-init` должны завершиться со статусом `Exited (0)`. Остальные сервисы должны быть в состоянии `Up`.

После запуска доступны:

- Airflow UI: <http://localhost:8081>, логин `admin`, пароль `admin`;
- Kafka UI: <http://localhost:8080>;
- PostgreSQL: `localhost:5432`, база `fraud_detection`.

Отправьте 100 тестовых транзакций в Kafka:

```bash
docker compose --profile manual run --rm transaction-producer
```

## Проверка данных

Последние результаты скоринга:

```bash
docker compose exec postgres psql \
  -U pipeline_user -d fraud_detection \
  -c "SELECT transaction_id, score, fraud_flag, model_version, created_at FROM transaction_scores ORDER BY created_at DESC LIMIT 10;"
```

Количество строк по версиям модели:

```bash
docker compose exec postgres psql \
  -U pipeline_user -d fraud_detection \
  -c "SELECT model_version, COUNT(*) FROM transaction_scores GROUP BY model_version ORDER BY model_version;"
```

Количество строк по месяцам размеченной витрины:

```bash
docker compose exec postgres psql \
  -U pipeline_user -d fraud_detection \
  -c "SELECT batch_month, COUNT(*) FROM labeled_transactions GROUP BY batch_month ORDER BY batch_month;"
```

Список топиков Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --list
```

## Проверка DAG

DAG доступны в Airflow UI и запускаются по расписанию раз в 5 минут. Для всех DAG установлено `catchup=False`.

Добавьте следующий месяц в витрину:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger maintain_labeled_data
```

Запустите переобучение:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger retrain_fraud_model
```

Запустите проверку качества данных:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger check_data_quality
```

Проверить состояние запусков можно в Airflow UI или командой:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d retrain_fraud_model --no-backfill
```

Проверка ошибок импорта:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

## Переобучение модели

DAG `retrain_fraud_model` содержит последовательность задач:

```text
resolve_training_window -> prepare_training_data -> train_candidate
-> evaluate_candidate -> publish_candidate -> advance_training_state
```

Параметры контура:

- окно обучения содержит 3 календарных месяца;
- первые 2 месяца используются для train, последний месяц - для test;
- ежемесячный батч содержит до 5 000 строк;
- глубина хранения витрины составляет 12 месяцев;
- inference и обучение выполняются на CPU.

Повторный запуск `maintain_labeled_data` использует детерминированные UUID и не создает дубликаты. Указатель `last_trained_month` обновляется только после успешного сохранения модели. Если следующий месяц еще не сформирован, запуск переобучения завершается без ошибки со статусом `skipped` для задач, которым не хватает данных.

Опубликованные модели:

```bash
ls -la shared/models
```

Текущее состояние окна обучения:

```bash
docker compose exec postgres psql \
  -U pipeline_user -d fraud_detection \
  -c "SELECT * FROM pipeline_state;"
```

## Проверка качества модели

Основная метрика публикации - ROC-AUC на самом свежем месяце окна. Если значение ниже метрики текущей production-модели, новый файл не публикуется и указатель месяца не изменяется. Дополнительно рассчитываются Average Precision и F1.

Модель и `latest.json` сохраняются атомарно. `fraud-detector` проверяет манифест перед каждым сообщением и подхватывает новую версию без перезапуска контейнера.

## Data Quality

DAG `check_data_quality` проверяет:

- наличие строк в витрине;
- уникальность `transaction_id`;
- значения `target` из множества `{0, 1}`;
- заполненность и неотрицательность `amount`;
- заполненность `transaction_time`;
- непрерывность календарных месяцев;
- допустимые диапазоны широты и долготы;
- заполненность `cat_id` и `us_state`.

HTML-отчеты сохраняются в `shared/reports`. Актуальный отчет доступен в `shared/reports/latest.html`.

## Preprocessing

Общий модуль `common/features.py` используется при обучении и при скоринге. В нем реализованы:

- логарифмирование суммы, населения города и расстояния;
- расчет расстояния Haversine между клиентом и продавцом;
- циклические признаки часа и дня недели;
- признак выходного дня;
- обработка неизвестных категорий через `OneHotEncoder`;
- заполнение числовых пропусков и стандартизация.

Для классификации используется `LogisticRegression` с балансировкой классов.

## Тесты

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```

## Структура проекта

```text
.
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── common/
│   └── features.py
├── dags/
│   ├── check_data_quality.py
│   ├── maintain_labeled_data.py
│   └── retrain_fraud_model.py
├── data/
│   ├── test_sample.csv
│   └── train_seed.csv
├── database/
│   └── init.sql
├── fraud_detector/
├── ml_pipeline/
├── score_writer/
├── shared/
│   ├── models/
│   └── reports/
├── transaction_producer/
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Остановка

Остановить контейнеры:

```bash
docker compose down
```

Удалить контейнеры вместе с данными Kafka, PostgreSQL и Airflow:

```bash
docker compose down -v
```
