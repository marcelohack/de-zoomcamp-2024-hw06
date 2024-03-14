import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

def is_kafka_connected():
    print(producer.bootstrap_connected())

if __name__ == "__main__":
    is_kafka_connected()
