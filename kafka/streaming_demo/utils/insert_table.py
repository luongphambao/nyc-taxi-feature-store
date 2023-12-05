import os
import random
from datetime import datetime
from time import sleep
import pandas as pd 
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()

TABLE_NAME = "nyc_taxi"
NUM_ROWS = 1000


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    kafka=os.getenv("DATA_KAFKA")
    kafka_df=pd.read_parquet(kafka)
    print(kafka_df.head())
    raw_columns = kafka_df.columns
    #remove column lower case
    kafka_df.columns=[col.lower() for col in raw_columns]

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
    kafka_df=kafka_df[columns]
    # Loop over all columns and create random values
    for _ in range(NUM_ROWS):
        # Randomize values for feature columns
        row=kafka_df.sample()
        ##convert to list
        feature_values = row.values.tolist()[0]
        
        #convert timestamp to string
        feature_values[1]=str(feature_values[1])
        feature_values[2]=str(feature_values[2])
        print(feature_values)
        data=feature_values
        #print(data)
        #data = [0, datetime.now().strftime("%d/%m/%Y %H:%M:%S")] + feature_values
        # Insert data
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(data)}
        """
        pc.execute_query(query)
        sleep(2)


if __name__ == "__main__":
    main()
