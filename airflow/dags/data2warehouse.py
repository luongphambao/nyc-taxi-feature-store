from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
import pandas as pd
import os
DATA_DIR="/opt/airflow/data/"
POSTGRES_CONN_ID = "postgres_default" 




def insert_table():
    pg_hook = PostgresHook.get_hook(conn_id=POSTGRES_CONN_ID)
    for file in os.listdir(DATA_DIR):
        if file.endswith(".parquet") and str(file).startswith("yellow"):
            df=pd.read_parquet(os.path.join(DATA_DIR,file))
            print("Inserting data from file: "+file)
            #df=df.sample(1000)
            pg_hook.insert_rows(table="nyc_taxi_warehouse", rows=df.values.tolist())
    

with DAG(dag_id="nyc_taxi2", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql = """
        CREATE TABLE IF NOT EXISTS nyc_taxi_warehouse(
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
            congestion_surcharge FLOAT
        );
        
    """
    )

    insert_table_pg = PythonOperator(
        task_id="insert_table_pg",
        python_callable=insert_table,
        #op_kwargs={"copy_sql": "COPY nyc_taxi FROM STDIN WITH CSV HEADER DELIMITER AS ','"},
    )
    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="public.nyc_taxi",
        database="k6",
        expectation_suite_name="nyctaxi_suite",
        return_json_dict=True,
    )
 
create_table_pg>>insert_table_pg>>gx_validate_pg