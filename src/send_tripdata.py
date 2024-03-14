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
