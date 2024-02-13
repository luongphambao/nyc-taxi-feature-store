import os
import random
from datetime import datetime
from time import sleep
import pandas as pd 
from postgresql_client import PostgresSQLClient

TABLE_NAME = "nyc_taxi"
NUM_ROWS = 10000


def main():
    pc = PostgresSQLClient(
        database="k6",
        user="k6",
        password="k6",
        host="172.17.0.1"
    )

    kafka_df=pd.read_parquet("yellow_stream.parquet")
    #drop na
    kafka_df=kafka_df.dropna()
    #print(kafka_df.head())
    raw_columns = kafka_df.columns
    #remove column lower case
    kafka_df.columns=[col.lower() for col in raw_columns]

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
        print(len(columns))
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
    #kafka_df=kafka_df[columns]
    # Loop over all columns and create random values

    for _ in range(NUM_ROWS):
        # Randomize values for feature columns
        row=kafka_df.sample()
        ##convert to list
        feature_values = row.values.tolist()[0]
        
        #convert timestamp to string
        feature_values[1]=str(feature_values[1])
        feature_values[2]=str(feature_values[2])
        #print(feature_values)
        data=feature_values
        #print(data)
        #data = [0, datetime.now().strftime("%d/%m/%Y %H:%M:%S")] + feature_values
        # Insert data
        # query = f"""
        #     insert into {TABLE_NAME} ({",".join(columns)})
        #     values {tuple(data)}
        # """
        created_time=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        content="yellow"
        data=[created_time,content]+data
        #convert columns to list
        columns=list(columns)
        # columns.insert(0,"created")
        # columns.insert(1,"content")
        # print(data)
        # print(len(data))
        # print(columns)
        #exit()
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(data)}
        """
        pc.execute_query(query)
        sleep(1)


if __name__ == "__main__":
    main()
