import os
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient
load_dotenv(".env")


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    create_table_query = """
        CREATE TABLE IF NOT EXISTS nyc_taxi (
            vendorid  INT, 
            tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count FLOAT, 
            trip_distance FLOAT, 
            ratecodeid FLOAT, 
            store_and_fwd_flag VARCHAR(1), 
            pulocationid INT, 
            dolocationid INT, 
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
        );
    """
    try:
        pc.execute_query(create_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
