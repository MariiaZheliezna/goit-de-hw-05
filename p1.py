from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = AdminClient(kafka_config)

topics = [ 
        NewTopic("building_sensors_MZ", num_partitions=2, replication_factor=1), 
        NewTopic("temperature_alerts_MZ", num_partitions=2, replication_factor=1), 
        NewTopic("humidity_alerts_MZ", num_partitions=2, replication_factor=1) 
        ] 

# Створення нових топіків 
try: 
    admin_client.create_topics(topics, validate_only=False) 
    print("Топіки створені успішно")
except Exception as e:
    print(f"Помилка: {e}")

print(f"\nТри топіків з команди \n[print(topic) for topic in admin_client.list_topics() if '_MZ' in topic]:\n")
metadata = admin_client.list_topics(timeout=10)
topics = metadata.topics

[print(topic) for topic in topics if "_MZ" in topic]