import json

from confluent_kafka import Consumer, Producer
from configs import kafka_config, kafka_config_consumer
from configs import delivery_report

consumer = Consumer(kafka_config_consumer)
consumer.subscribe(['building_sensors_MZ'])

# Створення продюсера Kafka 
producer = Producer(kafka_config)

# Обробка повідомлень
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Помилка: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        sensor_id = data['sensor_id']
        timestamp = data['timestamp']
        temperature = data['temperature']
        humidity = data['humidity']

        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "Алярм!  Спека! Приміть міри для вашої безпеки!..."
            }
            producer.produce('temperature_alerts_MZ', value=json.dumps(alert).encode('utf-8'), callback=delivery_report)
            producer.poll(1)

        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Алярм! Вологість за межами дозволених значень! Небезпека!..."
            }
            producer.produce('humidity_alerts_MZ', value=json.dumps(alert).encode('utf-8'), callback=delivery_report)
            producer.poll(1)

        print(f"Оброблено: {data}")
except KeyboardInterrupt:
    print("Виконання закінчено...")
finally:
    consumer.close()
    producer.flush()
