import argparse
import time
import random as rnd
from kafka import KafkaProducer
 
import config
 
parser = argparse.ArgumentParser(description = "Генератор IoT-показаний в Kafka")
parser.add_argument("--rate", type = float, default = 1.0, help = "сообщений в секунду (по умолчанию 1)")
parser.add_argument("--count", type = int, default = 0, help = "сколько отправить (0 = бесконечно)")
args = parser.parse_args()
 
producer = KafkaProducer(
    bootstrap_servers = [config.KAFKA_BROKER],
    value_serializer = lambda v: v.encode('utf-8')
)
 
print(f"Generator started: {config.KAFKA_BROKER}, topic '{config.TOPIC_IN}', "
      f"rate={args.rate}/s, count={args.count or 'inf'}")
 
sent = 0
try:
    while args.count == 0 or sent < args.count:
        device_id = rnd.choice(config.DEVICE_TYPE_IDS)
        event_ts = int(time.time() * 1000) - rnd.randint(0, 2000)
        temperature = round(rnd.uniform(config.TEMP_MIN, config.TEMP_MAX), 1)
        humidity = round(rnd.uniform(config.HUM_MIN, config.HUM_MAX), 1)
        msg = ",".join(map(str, (device_id, event_ts, temperature, humidity)))
        producer.send(config.TOPIC_IN, value=msg)
        print(f"sent: {msg}")
        sent += 1
        time.sleep(1.0 / args.rate)
finally:
    producer.flush()
    producer.close()