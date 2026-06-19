# Заключительный проект: потоковая обработка показаний IoT-устройств на Apache Flink

## Задача

Проект реализует потоковую обработку данных от IoT-датчиков. Генератор раз в секунду
отправляет показания (температура, влажность) в Kafka. Apache Flink (PyFlink) читает
поток, обогащает каждое событие названием типа устройства из справочника в PostgreSQL
и раз в минуту по каждому типу устройства считает агрегаты, после чего публикует
результат обратно в Kafka. Отдельный потребитель читает результат, сохраняет его в
файл и в PostgreSQL, а также детектирует аномальные окна (тревоги).

Для каждого минутного окна и каждого типа устройства считаются:
- средняя температура (`avg_temp`);
- медиана влажности (`median_humidity`);
- минимальная и максимальная температура (`min_temp`, `max_temp`);
- количество замеров (`readings_count`);
- человекочитаемый статус (`status`): комфортно / жарко / холодно / влажно / сухо.

## Архитектура

```text
generator.py
    -> Kafka topic iot_in                       (входной поток показаний)
    -> Flink: Kafka source (DataStream API)
    -> парсинг + watermark (event time, допуск опоздания 5 сек)
    -> минутное tumbling-окно по типу устройства (avg, median, min, max, count)
    -> from_data_stream (переход DataStream -> Table API)
    -> UDF ComfortStatus (статус по температуре и влажности)
    -> lookup join со справочником device_types (PostgreSQL, JDBC)
    -> Kafka topic iot_out                       (выходной поток результатов)
consumer.py
    -> results.csv + PostgreSQL (iot_aggregates) + alerts.csv (тревоги)
```

Компоненты:
- **Kafka** — брокер сообщений: входной топик `iot_in`, выходной `iot_out`.
- **PostgreSQL** — справочник типов устройств `device_types` (источник для JOIN) и
  таблица результатов `iot_aggregates` (куда пишет потребитель).
- **PyFlink** — основная потоковая обработка.
- **Docker Compose** — поднимает Kafka и PostgreSQL локально.

## Выбор реализации (почему так)

- **Kafka** — буфер между генератором, Flink и потребителем результата.
- **PostgreSQL + lookup join.** Во входном событии есть только `device_type_id`, а
  название типа хранится в справочнике. Flink подключает таблицу через JDBC и делает
  lookup join (`FOR SYSTEM_TIME AS OF proc_time`), который даёт поток «только на
  вставку» — именно такой принимает приёмник Kafka.
- **Медиана считается в DataStream API.** Среднее можно посчитать SQL-агрегацией, но
  медиана во Flink SQL зависит от версии, поэтому окно и расчёт медианы вынесены в
  DataStream (медиана — явным сортированием), а вокруг стоят переходы Table↔DataStream.
- **Event time + watermark с допуском 5 секунд** — чтобы учитывать слегка запоздавшие
  события. Генератор добавляет небольшой разброс времени (jitter), чтобы это проверить.
- **Запись результата в Postgres — из потребителя (psycopg2), а не из Flink.** На Flink
  2.0 используемый JDBC-коннектор не поддерживает запись (sink), поэтому persistence
  вынесена в `consumer.py`. Это также делает потребителя самостоятельным сервисом-монитором.
- **Формат CSV** для сообщений Kafka — как в семинарах.

## Формат данных

Вход (`iot_in`, CSV): `device_type_id,event_ts_ms,temperature,humidity`
```text
2,1781889813234,22.0,59.6
```
Выход (`iot_out`, CSV):
`event_time,device_type,avg_temp,median_humidity,min_temp,max_temp,readings_count,status`
```text
20:22,Гигрометр,22.79,53.05,15.4,29.8,12,комфортно
```

## Справочник в PostgreSQL

DDL — `sql/ddl.sql` (создаёт `device_types` и `iot_aggregates`), наполнение — `sql/dml.sql`:
```text
1 -> Термостат
2 -> Гигрометр
3 -> Метеостанция
4 -> Датчик теплицы
```

## Структура проекта
```text
iot_flink_project_extended/
├── config.py                  # все настройки
├── metrics.py                 # чистые расчётные функции (average, median, format_minute, comfort_status) + самопроверка
├── generator.py               # генератор показаний в Kafka (argparse: --rate, --count)
├── flink_job_extended.py      # основной конвейер Flink
├── consumer.py                # читает iot_out -> results.csv + Postgres + alerts.csv
├── requirements.txt           # зависимости
├── utils/
│   ├── jars.py                # пути к jar-коннекторам
│   └── iot_event_time.py      # парсинг сообщений + извлечение времени события
├── sql/
│   ├── ddl.sql                # справочник + таблица результатов
│   └── dml.sql                # наполнение справочника
└── jars/                      # 3 jar-коннектора (kafka, jdbc, postgresql)
```

## Технологии
Python 3.11, PyFlink 2.0.2, Apache Kafka 3.9, PostgreSQL 16, Docker Compose,
`kafka-python`, `psycopg2-binary`.

## Запуск

```bash
# 0. Поднять Kafka и PostgreSQL
docker compose up -d        # docker-compose.yml — в базовом проекте; контейнеры общие

# 1. Окружение и зависимости
conda activate flink
pip install -r requirements.txt

# 2. Топики Kafka
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic iot_in  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic iot_out --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 3. Таблицы Postgres (справочник + результаты)
docker exec -i postgres psql -U postgres -d testdb < sql/ddl.sql
docker exec -i postgres psql -U postgres -d testdb < sql/dml.sql
```

Запуск конвейера — три терминала (в каждом: `conda activate flink && cd ~/iot_flink_project_extended`).
**Важен порядок: Flink первым.**
```bash
python flink_job_extended.py      # терминал 1 — конвейер (запускать первым)
python generator.py --rate 5      # терминал 2 — генератор
python consumer.py                # терминал 3 — потребитель (results.csv + Postgres + alerts.csv)
```
Первый результат появляется примерно через минуту после старта генератора (окно = 1 минута).

## Проверка результата

```bash
# 1) поток результатов в Kafka
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic iot_out --bootstrap-server localhost:9092 --from-beginning

# 2) результаты, сохранённые в Postgres
docker exec -it postgres psql -U postgres -d testdb -c "SELECT * FROM iot_aggregates ORDER BY event_time;"

# 3) файлы в папке проекта
cat results.csv      # все окна
cat alerts.csv       # только аномальные окна (тревоги)
```

## Самопроверка расчётов
```bash
python metrics.py    # -> metrics: все проверки пройдены
```

## Что реализовано из требований
1. Генератор показаний (тип, время, температура, влажность) → Kafka — `generator.py`
2. DDL/DML справочника типов в Postgres — `sql/ddl.sql`, `sql/dml.sql`
3. Источник Kafka (`src: kafka`) — `KafkaSource` (DataStream API)
4. Источник Postgres (`src: pg`) — `CREATE TABLE … 'connector'='jdbc'` (Table API)
5. JOIN событий со справочником — lookup join
6. Окно: средняя температура + медиана влажности за минуту — `IotWindowFunction`
7. Приёмник Kafka в нужном формате — `CREATE TABLE iot_out … 'connector'='kafka'`
8. Source/sink на SQL/Table API — pg-источник и kafka-приёмник на Table API
9. Переход DataStream ↔ Table API — `from_data_stream`
10. Работа в event time — watermark по времени события + `TumblingEventTimeWindows`

## Дополнительно (сверх минимума)
- доп. метрики окна: min, max, count;
- UDF `ComfortStatus` — статус по температуре и влажности;
- потребитель `consumer.py`: сохраняет результат в `results.csv` и в Postgres (`iot_aggregates`);
- детект аномалий: окна с экстремальными значениями попадают в `alerts.csv` с указанием причины;
- настройки в `config.py`, помощники в пакете `utils/`, расчётная логика в `metrics.py` с самопроверкой.
