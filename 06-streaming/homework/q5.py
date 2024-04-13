import json
import time 

from kafka import KafkaProducer

PATH = "/Users/jack/dev/data-engineering-zoomcamp-2024/data/green_tripdata_2019-10.csv.gz"

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

res = producer.bootstrap_connected()

t0 = time.time()

topic_name = 'green-trips'

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Taxi Data Processing") \
    .getOrCreate()

# Path to the gzipped CSV file
file_path = "/Users/jack/dev/data-engineering-zoomcamp-2024/data/green_tripdata_2019-10.csv.gz"

# Columns to be read
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# Reading the gzipped CSV with only the required columns
df_green = spark.read.option("header", "true").csv(file_path).select(columns)

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    message = json_serializer(row_dict)
    print(row_dict)
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")

tsend = time.time()

producer.flush()

t1 = time.time()
print(f'send took {(tsend - t0):.2f} seconds')
print(f'flush took {(t1 - tsend):.2f} seconds')
print(f'all took {(t1 - t0):.2f} seconds')
