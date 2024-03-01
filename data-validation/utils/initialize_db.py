import argparse
import os

import dotenv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

dotenv.load_dotenv()


def main(args):
    # Connect to the database
    conn = psycopg2.connect(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    cursor = conn.cursor()

    # Create the table
    create_sql = f"""CREATE TABLE {args.table_name} (
        VendorID INT NOT NULL,
        tpep_pickup_datetime TIMESTAMP NOT NULL,
        tpep_dropoff_datetime TIMESTAMP NOT NULL,
        passenger_count FLOAT,
        trip_distance FLOAT,
        RatecodeID FLOAT,
        store_and_fwd_flag CHAR(1),
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT
    )"""
    try:
        cursor.execute(create_sql)
        print(f"Table {args.table_name} has been created successfully!")
        conn.commit()
    except Exception as e:
        print(f"Creation failed with error: {e}")

    # Insert to table
    df = pd.read_parquet(args.input_file)
    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
            os.getenv("POSTGRES_HOST"),
            os.getenv("POSTGRES_PORT"),
            os.getenv("POSTGRES_DB"),
        )
    )
    try:
        df.to_sql(args.table_name, con=engine.connect(), if_exists="replace")
        print(f"Table {args.table_name} has been inserted successfully!")
    except Exception as e:
        print(f"Insertion failed with error {e}")

    # Closing the connection
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--table_name",
        default="yellow_tripdata",
        help="Table name of the generated data.",
    )
    parser.add_argument(
        "-i",
        "--input_file",
        default="./data/yellow_tripdata/yellow_tripdata_2022-12.parquet",
        type=str,
        help="Path to the data file in parquet format.",
    )
    args = parser.parse_args()

    main(args)
