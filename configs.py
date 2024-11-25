kafka_config = {
    "bootstrap.servers": '77.81.230.104:9092',
    "sasl.username": 'admin',
    "sasl.password": 'VawEzo1ikLtrA8Ug8THa',
    "security.protocol": 'SASL_PLAINTEXT',
    "sasl.mechanism": 'PLAIN'
}

kafka_config_consumer = {
    "bootstrap.servers": '77.81.230.104:9092',
    "sasl.username": 'admin',
    "sasl.password": 'VawEzo1ikLtrA8Ug8THa',
    "security.protocol": 'SASL_PLAINTEXT',
    "sasl.mechanism": 'PLAIN',
    "group.id": "sensor-group",
    "auto.offset.reset": "earliest"
}

def delivery_report(err, msg): 
    if err is not None: 
        print(f"Помилка відправки: {msg.key()}: {err}") 
    else: 
        print(f"Відправлено: {msg.topic()} [{msg.partition()}]")