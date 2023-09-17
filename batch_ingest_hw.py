#to be run on EC2 instance
import s3fs
from s3fs.core import S3FileSystem
import numpy as np
import pickle
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile #to unzip the files

import pandas as pd



def ingest_data():

    #define kaggle api
    api = KaggleApi()
    api.authenticate()

    #get the file and unzip it
    api.dataset_download_file('usdot/flight-delays', file_name='flights.csv')  # dataset, file name
    with zipfile.ZipFile('flights.csv.zip', 'r') as zipref:
        zipref.extractall()



    # All the data is stored in a pandas dataframe called data, create the data frame here
    #utilize only these columns from the csv file downloaded
    columns_to_read = ['YEAR'
                       ,'MONTH'
                       ,'AIRLINE'
                       ,'FLIGHT_NUMBER'
                       ,'ORIGIN_AIRPORT'
                       ,'DESTINATION_AIRPORT'
                       ,'DEPARTURE_DELAY'
                       ,'CANCELLED'
                       ]


    #specify the columns to be read from the file
    df = pd.read_csv('flights.csv',usecols=columns_to_read)

    #Define the filtering conditions
    target_airlines = ["AA", "US"]
    target_airport_origin_code = 'CLT'
    target_month = 1

    #filter the dataframe based on criteria
    filtered_df = df[(df['MONTH'] == target_month) & (df['ORIGIN_AIRPORT'] == target_airport_origin_code) & (df['AIRLINE'].isin(target_airlines))]


    #print(filtered_df)


    s3 = S3FileSystem()
    # S3 bucket directory
    DIR = 's3://ece5984-bucket-whb7/HW1/Batch'
    with s3.open('{}/{}'.format(DIR, 'filtered_df.pkl'), 'wb') as f:
        f.write(pickle.dumps(filtered_df))


ingest_data()


#/usr/local/lib/python3.8/dist-packages