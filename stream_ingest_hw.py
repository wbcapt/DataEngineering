import time
from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem


def kafka_consumer():
    s3 = S3FileSystem()
    DIR = "s3://ece5984-bucket-whb7/HW1/Stream"            # Add S3 bucket location
    t_end = time.time() + 180 * 1  # Amount of time data is sent for
    while time.time() < t_end:
        consumer = KafkaConsumer(
            'FlightData',  # add Topic name here
            bootstrap_servers=['54.196.246.52:9108'],  # add your IP and port number here
            value_deserializer=lambda x: loads(x.decode('utf-8')))

        for count, i in enumerate(consumer):
            with s3.open("{}/flight_data_{}.json".format(DIR, count),
                         'w') as file:
                json.dump(i.value, file)
    print("done consuming")