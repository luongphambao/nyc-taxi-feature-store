import os

import dotenv
import pandas as pd
from sqlalchemy import create_engine

dotenv.load_dotenv(".env")


def main():
    # df = pd.read_csv('yellow_tripdata_2021-01.csv',parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    db_name = os.getenv("POSTGRES_DB")
    print(user, password, host, db_name)
    engine = create_engine(
        "postgresql://" + user + ":" + password + "@" + host + ":5432/" + db_name
    )
    year = 2021
    df = pd.read_csv("yellow_tripdata_2021-01.csv")
    print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


if __name__ == "__main__":
    main()
