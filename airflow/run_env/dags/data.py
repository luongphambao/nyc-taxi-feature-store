from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(dag_id="nyc_taxi", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        #bash_command='apt-get update && apt-get upgrade -y'
        bash_command='echo "Install some pypi libs..."',
    )

    # https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
    @task.virtualenv(
        task_id="download_nyc_data_2020",
        requirements=["requests","pandas","numpy"],
        system_site_packages=False,
    )
    def download_nyc_data_2020():
        import requests
        import os 
        import pandas as pd
        import numpy as np

        DATA_DIR = "/opt/airflow/data/2020"
        os.makedirs(DATA_DIR, exist_ok=True)
        months=["01","02","03","04","05","06","07","08","09","10","11","12"]
        data_type1="green_tripdata_"
        data_type2="yellow_tripdata_"
        url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        for month in months:
            url_download=url_prefix+data_type1+"2020-"+month+".parquet"
            print(url_download)
            file_path=os.path.join(DATA_DIR,data_type1+"2020-"+month+".parquet")
            if os.path.exists(file_path):
                print("File already exists: "+file_path)
                continue
            try:
                r = requests.get(url_download, allow_redirects=True)
                open(file_path, 'wb').write(r.content)
            except:
                print("Error in downloading file: "+url_download)
                continue
    @task
    def merge_df():
        import os 
        import pandas as pd
        DATA_DIR="/opt/airflow/data/2020"
        data_type1="green_tripdata_"
        data_type2="yellow_tripdata_"
        root="/opt/airflow/data/"
        months=["01","02","03","04","05","06","07","08","09","10","11","12"]
        list_parquet1=[os.path.join(DATA_DIR,data_type1+"2020-"+month+".parquet") for month in months]
        #list_parquet2=[os.path.join(DATA_DIR,data_type2+"2020-"+month+".parquet") for month in months]
        list_df1=[pd.read_parquet(parquet) for parquet in list_parquet1]
        #list_df2=[pd.read_parquet(parquet) for parquet in list_parquet2]
        df1=pd.concat(list_df1)
        #df2=pd.concat(list_df2)
        df1.to_parquet(root+"green_tripdata_2020.parquet")
        #df2.to_parquet(root+"/yellow_tripdata_2020.parquet")
        #check missing values
    @task
    def drop_not_use_column():
        import pandas as pd
        data_path1="/opt/airflow/data/green_tripdata_2020.parquet"
        data_path2="/opt/airflow/data/yellow_tripdata_2020.parquet"
        #drop ehail_fee green
        df_green=pd.read_parquet(data_path1)
        df_green=df_green.drop(columns=["ehail_fee"])
        #drop airport fee yellow
        #df_yellow=df_yellow.drop(columns=["airport_fee"])
        df_green.to_parquet(data_path1)
        #df_yellow.to_parquet(data_path2)
    @task 
    def drop_missing_column():
        import pandas as pd
        data_path1="/opt/airflow/data/green_tripdata_2020.parquet"
        data_path2="/opt/airflow/data/yellow_tripdata_2020.parquet"
        df_green=pd.read_parquet(data_path1)
        df_yellow=pd.read_parquet(data_path2)
        #check missing values
        missing_green=df_green.isnull().sum()
        missing_yellow=df_yellow.isnull().sum()
        #drop missing values
        df_green=df_green.dropna()
        df_yellow=df_yellow.dropna()
        df_green.to_parquet(data_path1)
        #df_yellow.to_parquet(data_path2)
    system_maintenance_task >>download_nyc_data_2020() >> merge_df() >> drop_not_use_column()>>drop_missing_column()
