import os

from postgresql_client import PostgresSQLClient


def main():
    print(os.getenv("POSTGRES_DB"))
    # pc = PostgresSQLClient(
    #     database=os.getenv("POSTGRES_DB"),
    #     user=os.getenv("POSTGRES_USER"),
    #     password=os.getenv("POSTGRES_PASSWORD"),
    # )
    pc = PostgresSQLClient(
        database="k6", user="k6", password="k6", port="5432", host="172.17.0.1"
    )
    # Create devices table

    create_table_query = """
        CREATE TABLE IF NOT EXISTS nyc_taxi (
            created TIMESTAMP WITHOUT TIME ZONE,
            vendorid  INT, 
            pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
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
            content VARCHAR(30)
        );
    """
    try:
        pc.execute_query(create_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
