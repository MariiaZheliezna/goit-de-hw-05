import json

from confluent_kafka import Consumer
from configs import kafka_config_consumer


temperature_consumer = Consumer(kafka_config_consumer)
temperature_consumer.subscribe(['temperature_alerts_MZ'])
humidity_consumer = Consumer(kafka_config_consumer)
humidity_consumer.subscribe(['humidity_alerts_MZ'])

try:
    while True:
        temp_msg = temperature_consumer.poll(1.0)
        if temp_msg is not None:
            if temp_msg.error():
                print(f"Помилка зчитування температури: {temp_msg.error()}")
            else:
                temp_alert = json.loads(temp_msg.value().decode('utf-8'))
                print(f"Температурний алярм: \n{temp_alert}")

        humid_msg = humidity_consumer.poll(1.0)
        if humid_msg is not None:
            if humid_msg.error():
                print(f"Помилка зчитування вологості: {humid_msg.error()}")
            else:
                humid_alert = json.loads(humid_msg.value().decode('utf-8'))
                print(f"Алярм вологості: \n{humid_alert}")

except KeyboardInterrupt:
    print("Виконання закінчено...")
finally:
    temperature_consumer.close()
    humidity_consumer.close()