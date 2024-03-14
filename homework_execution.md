
## Question 1: Redpanda version

```console
❯ docker compose -f docker/docker-compose.yaml exec -it redpanda-1 bash
redpanda@ae391f68b442:/$ rpk help
rpk is the Redpanda CLI & toolbox

Usage:
  rpk [command]

Available Commands:
  acl         Manage ACLs and SASL users
  cloud       Interact with Redpanda cloud
  cluster     Interact with a Redpanda cluster
  container   Manage a local container cluster
  debug       Debug the local Redpanda process
  generate    Generate a configuration template for related services
  group       Describe, list, and delete consumer groups and manage their offsets
  help        Help about any command
  iotune      Measure filesystem performance and create IO configuration file
  plugin      List, download, update, and remove rpk plugins
  redpanda    Interact with a local Redpanda process
  topic       Create, delete, produce to and consume from Redpanda topics
  version     Check the current version
  wasm        Deploy and remove inline WASM engine scripts

Flags:
  -h, --help      Help for rpk
  -v, --verbose   Enable verbose logging (default: false)

Use "rpk [command] --help" for more information about a command.
redpanda@ae391f68b442:/$ rpk version
v22.3.5 (rev 28b2443)
redpanda@ae391f68b442:/$
```

## Question 2. Creating a topic

```console
rpk topic help
Create, delete, produce to and consume from Redpanda topics

Usage:
  rpk topic [command]

Available Commands:
  add-partitions Add partitions to existing topics
  alter-config   Set, delete, add, and remove key/value configs for a topic
  consume        Consume records from topics
  create         Create topics
  delete         Delete topics
  describe       Describe a topic
  list           List topics, optionally listing specific topics
  produce        Produce records to a topic

Flags:
      --brokers strings         Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092'). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses
      --config string           Redpanda config file, if not set the file will be searched for in the default locations
  -h, --help                    Help for topic
      --password string         SASL password to be used for authentication
      --sasl-mechanism string   The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512
      --tls-cert string         The certificate to be used for TLS authentication with the broker
      --tls-enabled             Enable TLS for the Kafka API (not necessary if specifying custom certs)
      --tls-key string          The certificate key to be used for TLS authentication with the broker
      --tls-truststore string   The truststore to be used for TLS communication with the broker
      --user string             SASL user to be used for authentication

Global Flags:
  -v, --verbose   Enable verbose logging (default: false)

Use "rpk topic [command] --help" for more information about a command.
redpanda@ae391f68b442:/$ rpk topic create test-topic
TOPIC       STATUS
test-topic  OK
```

## Question 3. Connecting to the Kafka server

Create script src/kafka_connect.py  

```python
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
    
```
Run script  

```console
python src/kafka_connect.py
True

```

## Question 4. Sending data to the stream

Create script src/kafka_send.py  
```python
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
```

Run script  

```console
python src/kafka_send.py
Sent: {'number': 0}
Sent: {'number': 1}
Sent: {'number': 2}
Sent: {'number': 3}
Sent: {'number': 4}
Sent: {'number': 5}
Sent: {'number': 6}
Sent: {'number': 7}
Sent: {'number': 8}
Sent: {'number': 9}
took before flush 0.55 seconds
took after flush 0.55 seconds
```

#### How much time did it take? Where did it spend most of the time?

Sending the messages
Flushing
**Both took approximately the same amount of time**
(Don't remove time.sleep when answering this question)

#### Reading data with rpk

```console
redpanda@b43f7645ee99:/$ rpk topic consume test-topic
{
  "topic": "test-topic",
  "value": "{\"number\": 0}",
  "timestamp": 1710296792570,
  "partition": 0,
  "offset": 0
}
{
  "topic": "test-topic",
  "value": "{\"number\": 1}",
  "timestamp": 1710296792626,
  "partition": 0,
  "offset": 1
}
{
  "topic": "test-topic",
  "value": "{\"number\": 2}",
  "timestamp": 1710296792681,
  "partition": 0,
  "offset": 2
}
{
  "topic": "test-topic",
  "value": "{\"number\": 3}",
  "timestamp": 1710296792737,
  "partition": 0,
  "offset": 3
}
{
  "topic": "test-topic",
  "value": "{\"number\": 4}",
  "timestamp": 1710296792793,
  "partition": 0,
  "offset": 4
}
{
  "topic": "test-topic",
  "value": "{\"number\": 5}",
  "timestamp": 1710296792848,
  "partition": 0,
  "offset": 5
}
{
  "topic": "test-topic",
  "value": "{\"number\": 6}",
  "timestamp": 1710296792904,
  "partition": 0,
  "offset": 6
}
{
  "topic": "test-topic",
  "value": "{\"number\": 7}",
  "timestamp": 1710296792958,
  "partition": 0,
  "offset": 7
}
{
  "topic": "test-topic",
  "value": "{\"number\": 8}",
  "timestamp": 1710296793014,
  "partition": 0,
  "offset": 8
}
{
  "topic": "test-topic",
  "value": "{\"number\": 9}",
  "timestamp": 1710296793066,
  "partition": 0,
  "offset": 9
}
{
  "topic": "test-topic",
  "value": "{\"number\": 0}",
  "timestamp": 1710296852242,
  "partition": 0,
  "offset": 10
}
{
  "topic": "test-topic",
  "value": "{\"number\": 1}",
  "timestamp": 1710296852297,
  "partition": 0,
  "offset": 11
}
{
  "topic": "test-topic",
  "value": "{\"number\": 2}",
  "timestamp": 1710296852351,
  "partition": 0,
  "offset": 12
}
{
  "topic": "test-topic",
  "value": "{\"number\": 3}",
  "timestamp": 1710296852407,
  "partition": 0,
  "offset": 13
}
{
  "topic": "test-topic",
  "value": "{\"number\": 4}",
  "timestamp": 1710296852460,
  "partition": 0,
  "offset": 14
}
{
  "topic": "test-topic",
  "value": "{\"number\": 5}",
  "timestamp": 1710296852513,
  "partition": 0,
  "offset": 15
}
{
  "topic": "test-topic",
  "value": "{\"number\": 6}",
  "timestamp": 1710296852566,
  "partition": 0,
  "offset": 16
}
{
  "topic": "test-topic",
  "value": "{\"number\": 7}",
  "timestamp": 1710296852621,
  "partition": 0,
  "offset": 17
}
{
  "topic": "test-topic",
  "value": "{\"number\": 8}",
  "timestamp": 1710296852677,
  "partition": 0,
  "offset": 18
}
{
  "topic": "test-topic",
  "value": "{\"number\": 9}",
  "timestamp": 1710296852731,
  "partition": 0,
  "offset": 19
}

```

### Sending the taxi data
Create script src/read_tripdata.py
```python
import json
import pandas as pd

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

# Must specify the data types for the columns to avoid the warning: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
green_taxi_dtype = {
    # lpep_pickup_datetime, lpep_dropoff_datetime will be parsed as datetime
    'VendorID': pd.Int64Dtype(),
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': pd.Int64Dtype(),
    'trip_type': pd.Int64Dtype(),
    'congestion_surcharge': float
}

dataset_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

def get_tripdata(url) -> pd.DataFrame:
    df_green = pd.read_csv(url, sep=',', compression='gzip', dtype=green_taxi_dtype, parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
    return df_green[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']]

def read_tripdata(df_green):
    for row in df_green.itertuples(index=False):
        row_dict = {col: getattr(row, col) for col in row._fields}
        print(row_dict)
        break
        
if __name__ == "__main__":
    read_tripdata(get_tripdata(dataset_url))
```

Run script  

```console
python src/read_tripdata.py
{'lpep_pickup_datetime': '2019-10-01 00:26:02', 'lpep_dropoff_datetime': '2019-10-01 00:39:58', 'PULocationID': 112, 'DOLocationID': 196, 'passenger_count': 1, 'trip_distance': 5.88, 'tip_amount': 0.0}
```

## Question 5: Sending the Trip Data

### Create a topic green-trips and send the data there

```console
redpanda@b43f7645ee99:/$ rpk topic create green-trips
TOPIC        STATUS
green-trips  OK

```
### How much time in seconds did it take? (You can round it to a whole number)

Create script src/send_tripdata.py
```python
import json
import time
import numpy as np
import pandas as pd

from kafka import KafkaProducer

def pandas_df_json_encoder(value):
    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, np.int64):
        return int(value)
    elif isinstance(value, pd._libs.missing.NAType):
        return None
    raise TypeError(f'Object of type {type(value)} is not JSON serializable')

def json_serializer(data):
    return json.dumps(data, default=pandas_df_json_encoder).encode('utf-8')

server = 'localhost:9092'

# Must specify the data types for the columns to avoid the warning: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
green_taxi_dtype = {
    # lpep_pickup_datetime and lpep_dropoff_datetime will be converted to Timestamp using parse_dates
    'VendorID': pd.Int64Dtype(),
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': pd.Int64Dtype(),
    'trip_type': pd.Int64Dtype(),
    'congestion_surcharge': float
}

dataset_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'

producer = KafkaProducer(
    bootstrap_servers=[server],
    # key_serializer=str.encode,
    value_serializer=json_serializer
)

def get_tripdata(url) -> pd.DataFrame:
    t1 = time.time()
    df_green = pd.read_csv(url, sep=',', compression='gzip', dtype=green_taxi_dtype, parse_dates=[
                           'lpep_pickup_datetime', 'lpep_dropoff_datetime'])
    t2 = time.time()
    print(f'took {(t2 - t1):.2f} seconds to load the data')
    return df_green[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']]

def get_tripdata_key(data):
    return 

def send_tripdata(df_green, topic_name='green-trips'):
    if producer.bootstrap_connected():
        t0 = time.time()
        for row in df_green.itertuples(index=False):
            row_dict = {col: getattr(row, col) for col in row._fields}
            print(row_dict)
            producer.send(topic_name, value=row_dict)
            # producer.send(topic_name, value=row_dict, key=f"{row_dict['PULocationID']}_{row_dict['lpep_pickup_datetime'].strftime('%Y%m%d%H%M%S')}")

        t1 = time.time()
        print(f'took before flush {(t1 - t0):.0f} seconds')
        producer.flush()
        t2 = time.time()
        print(f'took after flush {(t2 - t0):.0f} seconds')
    else:
        print('Kafka is not connected')

if __name__ == "__main__":
    tt0 = time.time()
    send_tripdata(get_tripdata(dataset_url))
    tt2 = time.time()
    print(f'took {(tt2 - tt0):.0f} seconds')
```

Run script  

```console
python src/read_tripdata.py

...
{'lpep_pickup_datetime': Timestamp('2019-10-31 23:42:00'), 'lpep_dropoff_datetime': Timestamp('2019-10-31 23:56:00'), 'PULocationID': 76, 'DOLocationID': 39, 'passenger_count': <NA>, 'trip_distance': 3.08, 'tip_amount': 0.0}
{'lpep_pickup_datetime': Timestamp('2019-10-31 23:23:00'), 'lpep_dropoff_datetime': Timestamp('2019-10-31 23:56:00'), 'PULocationID': 56, 'DOLocationID': 215, 'passenger_count': <NA>, 'trip_distance': 6.84, 'tip_amount': 0.0}
took before flush 44 seconds
took after flush 44 seconds
took 49 seconds```

### Creating the PySpark consumer

## Question 6. Parsing the data

Check the notebooks/hw06_question6.ipynb notebook

```console
...
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
|lpep_pickup_datetime|lpep_dropoff_datetime|PULocationID|DOLocationID|passenger_count|trip_distance|tip_amount|
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
| 2019-10-01 00:26:02|  2019-10-01 00:39:58|         112|         196|              1|         5.88|       0.0|
| 2019-10-01 00:18:11|  2019-10-01 00:22:38|          43|         263|              1|          0.8|       0.0|
| 2019-10-01 00:09:31|  2019-10-01 00:24:47|         255|         228|              2|          7.5|       0.0|
| 2019-10-01 00:37:40|  2019-10-01 00:41:49|         181|         181|              1|          0.9|       0.0|
| 2019-10-01 00:08:13|  2019-10-01 00:17:56|          97|         188|              1|         2.52|      2.26|
| 2019-10-01 00:35:01|  2019-10-01 00:43:40|          65|          49|              1|         1.47|      1.86|
| 2019-10-01 00:28:09|  2019-10-01 00:30:49|           7|         179|              1|          0.6|       1.0|
| 2019-10-01 00:28:26|  2019-10-01 00:32:01|          41|          74|              1|         0.56|       0.0|
| 2019-10-01 00:14:01|  2019-10-01 00:26:16|         255|          49|              1|         2.42|       0.0|
| 2019-10-01 00:03:03|  2019-10-01 00:17:13|         130|         131|              1|          3.4|      2.85|
| 2019-10-01 00:07:10|  2019-10-01 00:23:38|          24|          74|              3|         3.18|       0.0|
| 2019-10-01 00:25:48|  2019-10-01 00:49:52|         255|         188|              1|          4.7|       1.0|
| 2019-10-01 00:03:12|  2019-10-01 00:14:43|         129|         160|              1|          3.1|       0.0|
| 2019-10-01 00:44:56|  2019-10-01 00:51:06|          18|         169|              1|         1.19|      0.25|
| 2019-10-01 00:55:14|  2019-10-01 01:00:49|         223|           7|              1|         1.09|      1.46|
| 2019-10-01 00:06:06|  2019-10-01 00:11:05|          75|         262|              1|         1.24|      2.01|
| 2019-10-01 00:00:19|  2019-10-01 00:14:32|          97|         228|              1|         3.03|      3.58|
| 2019-10-01 00:09:31|  2019-10-01 00:20:41|          41|          74|              1|         2.03|      2.16|
| 2019-10-01 00:30:36|  2019-10-01 00:34:30|          41|          42|              1|         0.73|      1.26|
| 2019-10-01 00:58:32|  2019-10-01 01:05:08|          41|         116|              1|         1.48|       0.0|
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
...
```

## Question 7: Most popular destination

Check the notebooks/hw06_question7.ipynb notebook

```console
...
-------------------------------------------
Batch: 18
-------------------------------------------
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|74          |17741|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|42          |15942|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|41          |14061|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|75          |12840|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|129         |11930|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|7           |11533|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|166         |10845|
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|236         |7913 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|223         |7542 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|238         |7318 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|82          |7292 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|181         |7282 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|95          |7244 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|244         |6733 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|61          |6606 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|116         |6339 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|138         |6144 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|97          |6050 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|49          |5221 |
|{2024-03-14 15:30:00, 2024-03-14 15:35:00}|151         |5153 |
+------------------------------------------+------------+-----+
only showing top 20 rows
...
```
