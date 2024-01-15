from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
DATA_DIR="/opt/airflow/data/"
def insert_table():
    import pandas as pd
    import os
    pg_hook = PostgresHook.get_hook(conn_id=POSTGRES_CONN_ID)
    # for file in os.listdir(DATA_DIR):
    #     if file.endswith(".parquet"):
    #         df=pd.read_parquet(os.path.join(DATA_DIR,file))
    #         #drop store_and_fwd_flag and airport_fee
    #         df=df.drop(columns=["store_and_fwd_flag","airport_fee"])
    #         print("Inserting data from file: "+file)
    df=pd.read_parquet("/opt/airflow/data/yellow_tripdata_2021-03.parquet")
    df=df.drop(columns=["store_and_fwd_flag","airport_fee"])
    pg_hook.insert_rows(table="nyc_taxi", rows=df.values.tolist())
POSTGRES_CONN_ID = "postgres_default"
with DAG(dag_id="nyc_taxi_datawarehouse", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql = """
        DROP TABLE IF EXISTS nyc_taxi;
        CREATE TABLE IF NOT EXISTS nyc_taxi (
            vendorid  INT, 
            tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count FLOAT, 
            trip_distance FLOAT, 
            ratecodeid FLOAT,  
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
            congestion_surcharge FLOAT
        );
        
    """
    )

    insert_table_pg = PythonOperator(
        task_id="insert_table_pg",
        python_callable=insert_table,
        #op_kwargs={"copy_sql": "COPY nyc_taxi FROM STDIN WITH CSV HEADER DELIMITER AS ','"},
    )
 
create_table_pg