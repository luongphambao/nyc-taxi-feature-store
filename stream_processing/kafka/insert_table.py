import os
import random
from datetime import datetime
from time import sleep

import pandas as pd

from postgresql_client import PostgresSQLClient

TABLE_NAME = "nyc_taxi"
NUM_ROWS = 100000


def main():
    pc = PostgresSQLClient(
        database="k6",
        user="k6",
        password="k6",
        host="172.17.0.1",
    )

    kafka_df = pd.read_parquet("stream.parquet")

    # drop na
    # kafka_df=kafka_df.dropna()
    # print(kafka_df.head())
    raw_columns = kafka_df.columns
    # remove column lower case
    kafka_df.columns = [col.lower() for col in raw_columns]

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
        print(len(columns))
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    for _ in range(NUM_ROWS):
        # Randomize values for feature columns
        row = kafka_df.sample()
        ##convert to list
        feature_values = row.values.tolist()[0]

        feature_values[1] = str(feature_values[1])
        feature_values[2] = str(feature_values[2])
        created_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data = [created_time] + feature_values
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(data)}
        """
        pc.execute_query(query)
        sleep(1)


if __name__ == "__main__":
    main()
