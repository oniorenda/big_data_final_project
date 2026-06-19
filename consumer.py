"""
consumer.py - читает iot_out -> results.csv (+ Postgres, если доступен) + тревоги -> alerts.csv.
Postgres необязателен: если psycopg2 не установлен или БД недоступна, пишу только в CSV.
"""
import csv
from kafka import KafkaConsumer
 
import config
 
HEADER = ['event_time', 'device_type', 'avg_temp', 'median_humidity',
          'min_temp', 'max_temp', 'readings_count', 'status']
 
TEMP_ALERT_HIGH, TEMP_ALERT_LOW = 35.0, 10.0
HUM_ALERT_HIGH, HUM_ALERT_LOW = 75.0, 35.0
 
 
def alert_reason(avg_temp, median_hum):
    reasons = []
    if avg_temp >= TEMP_ALERT_HIGH:
        reasons.append('высокая температура')
    elif avg_temp <= TEMP_ALERT_LOW:
        reasons.append('низкая температура')
    if median_hum >= HUM_ALERT_HIGH:
        reasons.append('высокая влажность')
    elif median_hum <= HUM_ALERT_LOW:
        reasons.append('низкая влажность')
    return ', '.join(reasons) if reasons else None
 

cur = None
pg_ok = False
try:
    import psycopg2
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()
    pg_ok = True
    print(">>> Postgres: подключение OK")
except Exception as e:
    print(f">>> Postgres НЕдоступен ({e}). Пишу только в CSV.")
 
consumer = KafkaConsumer(
    config.TOPIC_OUT,
    bootstrap_servers=[config.KAFKA_BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda v: v.decode('utf-8'),
)
 
print(f">>> Kafka: читаю топик '{config.TOPIC_OUT}', жду сообщения ...")
 
results_file = open('results.csv', 'w', newline='', encoding='utf-8')
alerts_file = open('alerts.csv', 'w', newline='', encoding='utf-8')
rw = csv.writer(results_file)
aw = csv.writer(alerts_file)
rw.writerow(HEADER)
aw.writerow(HEADER + ['alert_reason'])
results_file.flush()
alerts_file.flush()
 
try:
    for message in consumer:
        line = message.value
        row = next(csv.reader([line]))
        if len(row) != 8:
            continue
 
        event_time, device_type, avg_temp, median_humidity, \
            min_temp, max_temp, readings_count, status = row
        avg_t = float(avg_temp)
        med_h = float(median_humidity)
 
        # 1) CSV
        rw.writerow(row)
        results_file.flush()
 
        # 2) Postgres (если доступен)
        if pg_ok:
            try:
                cur.execute(
                    f"INSERT INTO {config.PG_AGG_TABLE} "
                    "(event_time, device_type, avg_temp, median_humidity, "
                    "min_temp, max_temp, readings_count, status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (event_time, device_type, avg_t, med_h,
                     float(min_temp), float(max_temp), int(readings_count), status),
                )
            except Exception as e:
                print(f"   (pg insert error: {e})")
 
        # 3) тревога
        reason = alert_reason(avg_t, med_h)
        if reason:
            print(f"!!! ТРЕВОГА [{event_time} {device_type}]: {reason}")
            aw.writerow(row + [reason])
            alerts_file.flush()
        else:
            print(line)
finally:
    results_file.close()
    alerts_file.close()
    if pg_ok:
        cur.close()
        conn.close()