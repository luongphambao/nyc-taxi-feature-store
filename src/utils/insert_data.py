import os
import random
from datetime import datetime
from time import sleep
import pandas as pd 
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()

TABLE_NAME = "nyc_taxi"
NUM_ROWS = 10000


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port="5433"
    )
    print("connected to postgres")
    df=pd.read_parquet("/home/baolp/mlops/module2/MLE2/airflow/run_env/data/yellow_tripdata_2021-03.parquet")
    df=df.drop(columns=["store_and_fwd_flag","airport_fee"])
    columns=df.columns.tolist()
    for i in range(len(df)):
        row=df.iloc[i].values.tolist()
        row[1]=str(row[1])
        row[2]=str(row[2])
        print(row)
        data=row   
        #print(df.columns)
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(data)}
        """
        pc.execute_query(query)
    #sleep(2)


if __name__ == "__main__":
    main()
