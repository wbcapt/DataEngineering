from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API(...)
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer
from json import dumps

#this works

def kafka_producer():
    producer = KafkaProducer(bootstrap_servers=['54.196.246.52:9108'],  # change ip and port number here for kafka
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    time_counter = 0
    while time_counter < 36:
        #establish API Call to flight radar, return data, format data
        aircraft_id = "N927NN"  # targeted flight
        flights = fr_api.get_flights(
            airline="AA",  # american airlines
            registration=aircraft_id
        )
        listToStr = ''.join(str(elem) for elem in flights)
        formatted = listToStr.strip("[]<>").split(" - ")
        flight_data_list = formatted[1:].copy()
        flight_data_list.insert(0, f'ID: {aircraft_id}')
        flight_data = [item.split(": ")[1] for item in flight_data_list]

        #specify time to wai
        now = datetime.now()
        date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
        time_counter += 1
        time.sleep(5)

        #create the data frame for the data stream
        df_stream = pd.DataFrame(columns=["ID","Altitude", "Speed","Heading", "Timestamp","Counter"])
        new_rows = []

        new_row = {
                "ID": flight_data[0],
                "Altitude": flight_data[1],
                "Speed": flight_data[2],
                "Heading": flight_data[3],
                "Timestamp": date_time_str,
                "Counter": time_counter

            }
        new_rows.append(new_row)
        df_stream = pd.concat([df_stream, pd.DataFrame(new_rows)], ignore_index=True)
        #print(df_stream)
        producer.send('FlightData', value=df_stream.to_json())  # Add topic name here
    print("done producing")


kafka_producer()

#bin/kafka-topics.sh --create --topic FlightData --bootstrap-server 54.196.246.52:9108 --replication-factor 1 --partitions 1
#bin/kafka-console-consumer.sh --topic FlightData --bootstrap-server 54.196.246.52:9108