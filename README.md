# Заключительный проект: потоковая обработка показаний IoT-устройств на Apache Flink

## Задача

Проект реализует потоковую обработку данных от IoT-датчиков. Показания (температура,
влажность) раз в секунду поступают в Kafka. Apache Flink читает поток, обогащает каждое
событие названием типа устройства из справочника в PostgreSQL и раз в минуту считает по
каждому типу устройства агрегаты, после чего публикует результат обратно в Kafka и
дополнительно сохраняет его в PostgreSQL.

В каждом минутном окне по каждому типу устройства считаются:
- средняя температура (`avg_temp`);
- медиана влажности (`median_humidity`);
- минимальная и максимальная температура (`min_temp`, `max_temp`);
- количество замеров (`readings_count`);
- человекочитаемый статус (`status`): комфортно / жарко / холодно / влажно / сухо.

## Архитектура

```text
generator.py
    -> Kafka topic iot_in                    (входной поток показаний)
    -> Flink Kafka source (DataStream API)
    -> парсинг + watermark по event time
    -> минутное tumbling-окно по типу устройства (avg, median, min, max, count)
    -> from_data_stream (переход DataStream -> Table API)
    -> UDF ComfortStatus (статус по температуре и влажности)
    -> lookup join со справочником device_types (PostgreSQL, JDBC)
    -> StatementSet: запись в ДВА приёмника одновременно:
         * Kafka topic iot_out
         * PostgreSQL table iot_aggregates
consumer.py
    -> читает iot_out -> results.csv
```

Компоненты:
- **Kafka** — брокер сообщений: входной топик `iot_in`, выходной топик `iot_out`.
- **PostgreSQL** — справочник типов устройств `device_types` (источник) и таблица
  результатов `iot_aggregates` (приёмник).
- **PyFlink** — основная потоковая обработка.
- **Docker Compose** — поднимает Kafka и PostgreSQL локально.

## Выбор реализации (почему сделано именно так)

- **Kafka** — потому что датчиков много и они шлют данные независимо; Kafka выступает
  буфером между генератором, Flink и потребителем результата.
- **PostgreSQL + lookup join.** Во входном событии есть только `device_type_id` (число),
  а название типа хранится в справочнике. Flink подключает таблицу через JDBC и делает
  lookup join `JOIN device_types FOR SYSTEM_TIME AS OF a.proc_time`. Lookup join выбран,
  потому что он даёт поток «только на вставку» — именно такой принимает приёмник Kafka.
- **Медиана считается в DataStream API.** Среднее можно посчитать SQL-агрегацией, но
  медиана во Flink SQL зависит от версии. Поэтому окно и расчёт медианы вынесены в
  DataStream API (медиана — явным сортированием), а вокруг стоят переходы Table↔DataStream.
- **Два приёмника через StatementSet.** Результат одновременно уходит в Kafka (для
  дальнейшей обработки) и сохраняется в PostgreSQL (для хранения и проверок).
- **event time + watermark с допуском 5 секунд** (`for_bounded_out_of_orderness`) — чтобы
  корректно учитывать слегка запоздавшие события. Генератор специально добавляет небольшой
  разброс времени события (jitter), чтобы это проверить.
- **Формат CSV** для сообщений Kafka — как в семинарах (`'format' = 'csv'`).

## Формат данных

Входное сообщение в `iot_in` (CSV): `device_type_id,event_ts_ms,temperature,humidity`
```text
2,1781889813234,22.0,59.6
```
Выходное сообщение в `iot_out` (CSV):
`event_time,device_type,avg_temp,median_humidity,min_temp,max_temp,readings_count,status`
```text
20:22,Гигрометр,22.79,53.05,15.4,29.8,12,комфортно
```

## Справочник в PostgreSQL

DDL — `sql/ddl.sql`, наполнение — `sql/dml.sql`. Справочник:
```text
1 -> Термостат
2 -> Гигрометр
3 -> Метеостанция
4 -> Датчик теплицы
```

## Структура проекта
```text
iot_flink_project_extended/
├── docker-compose.yml
├── requirements.txt
├── pytest.ini
├── .gitignore
├── config.py                  # все настройки
├── metrics.py                 # чистые расчётные функции (покрыты тестами)
├── generator.py               # генератор показаний (argparse: --rate, --count)
├── flink_job_extended.py      # основной конвейер
├── flink_job.py               # базовая (упрощённая) версия — запасная
├── consumer.py                # читает iot_out -> results.csv
├── utils/
│   ├── jars.py                # пути к jar
│   └── iot_event_time.py      # парсинг сообщений + время события
├── sql/
│   ├── ddl.sql                # справочник + таблица результатов
│   └── dml.sql                # наполнение справочника
├── scripts/
│   ├── download_jars.sh       # скачивает jar-коннекторы
│   └── setup_db.sh            # создаёт топики и таблицы
├── tests/
│   └── test_metrics.py        # unit-тесты расчётной логики
└── jars/                      # 3 jar-коннектора
```

## Технологии
Python 3.11, PyFlink 2.0.2, Apache Kafka 3.9, PostgreSQL 16, Docker Compose,
`kafka-python`, `psycopg2-binary`, `pytest`.

## Запуск

```bash
docker compose up -d

conda activate flink
pip install -r requirements.txt

bash scripts/download_jars.sh
bash scripts/setup_db.sh
```

Запуск конвейера (каждое — в своём терминале, сначала `conda activate flink`):
```bash
python flink_job_extended.py
python generator.py --rate 5
python consumer.py
```

## Проверка результата

1. Результат в Kafka:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic iot_out --bootstrap-server localhost:9092 --from-beginning
```
2. Результат сохранён в PostgreSQL:
```bash
docker exec -it postgres psql -U postgres -d testdb -c "SELECT * FROM iot_aggregates ORDER BY event_time;"
```
3. Файл `results.csv` создаётся `consumer.py`.

## Автоматические тесты

Расчётная логика (среднее, медиана, формат времени, статус) вынесена в `metrics.py`
и покрыта unit-тестами:
```bash
pytest -q
```
Тесты проверяют: расчёт среднего; медиану для чётного и нечётного числа значений; формат
времени `HH:MM`; все категории статуса (жарко/холодно/влажно/сухо/комфортно/нет данных).

## Остановка
```bash
docker compose down
```

## Что расширено сверх минимума
- доп. метрики окна: min/max температуры и количество замеров;
- UDF `ComfortStatus` — статус по температуре и влажности;
- второй приёмник: результат сохраняется ещё и в PostgreSQL (`iot_aggregates`);
- запись в два приёмника одновременно через `StatementSet`;
- отдельный потребитель `consumer.py` -> `results.csv`;
- watermark с допуском опоздания (event time);
- расчётная логика вынесена в `metrics.py` и покрыта unit-тестами (`pytest`);
- скрипты автоматизации (`scripts/`), `requirements.txt`, `config.py`.