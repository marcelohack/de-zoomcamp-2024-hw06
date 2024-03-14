import json
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

def send_data():
    if producer.bootstrap_connected():
        t0 = time.time()

        topic_name = 'test-topic'

        for i in range(10):
            message = {'number': i}
            producer.send(topic_name, value=message)
            print(f"Sent: {message}")
            time.sleep(0.05)

        t1 = time.time()
        print(f'took before flush {(t1 - t0):.2f} seconds')

        producer.flush()

        t2 = time.time()
        print(f'took after flush {(t2 - t0):.2f} seconds')
    else:
        print('Kafka is not connected')

if __name__ == "__main__":
    send_data()