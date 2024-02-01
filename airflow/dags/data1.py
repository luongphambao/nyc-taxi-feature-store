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
    @task
    def download_nyc_data_yellow():
        import requests
        import os 
        import pandas as pd
        import numpy as np

        DATA_DIR = "/opt/airflow/data/"
        os.makedirs(DATA_DIR, exist_ok=True)
        print(os.listdir(DATA_DIR))
        years=["2021","2022"]
        months=["01","02","03","04","05","06","07","08","09","10","11","12"]
        data_type="yellow_tripdata_"

        url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        for year in years:
            for month in months:
                url_download=url_prefix+data_type+year+"-"+month+".parquet"
                print(url_download)
                file_path=os.path.join(DATA_DIR,data_type+year+"-"+month+".parquet")
                if os.path.exists(file_path):
                    print("File already exists: "+file_path)
                    continue
                try:
                    r = requests.get(url_download, allow_redirects=True)
                    open(file_path, 'wb').write(r.content)
                except:
                    print("Error in downloading file: "+url_download)
                    continue
    @task.virtualenv
    def download_nyc_data_green():
        import requests
        import os 
        import pandas as pd
        import numpy as np

        DATA_DIR = "/opt/airflow/data/"
        os.makedirs(DATA_DIR, exist_ok=True)
        print(os.listdir(DATA_DIR))
        years=["2021","2022"]
        months=["01","02","03","04","05","06","07","08","09","10","11","12"]
        data_type="green_tripdata_"

        url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        for year in years:
            for month in months:
                url_download=url_prefix+data_type+year+"-"+month+".parquet"
                print(url_download)
                file_path=os.path.join(DATA_DIR,data_type+year+"-"+month+".parquet")
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
    def drop_column():
        import pandas as pd
        import os
        data_path="/opt/airflow/data/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                df=pd.read_parquet(os.path.join(data_path,file))
                #check columns not have missing data
                df=df.dropna(axis=1,how='any')
                if "store_and_fwd_flag" in df.columns:
                    df=df.drop(columns=["store_and_fwd_flag"])
                    df.to_parquet(os.path.join(data_path,file))
                    print("Dropped column store_and_fwd_flag from file: "+file)
                else:
                    print("Column store_and_fwd_flag not found in file: "+file)
                    continue
    @task
    def drop_mssing_data():
        import pandas as pd
        import os
        data_path="/opt/airflow/data/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                df=pd.read_parquet(os.path.join(data_path,file))
                df=df.dropna()
                df.to_parquet(os.path.join(data_path,file))
                print("Dropped missing data from file: "+file)
    @task
    def create_streamming_data():
        import pandas as pd
        import os
        data_path="/opt/airflow/data/"
        streamming_path=os.path.join(data_path,"stream")
        os.makedirs(streamming_path, exist_ok=True)
        df_green_list=[]
        df_yellow_list=[]
        for file in os.listdir(data_path):
            df=pd.read_parquet(os.path.join(data_path,file))
            #get random 1000 rows
            print(df.shape)
            if df.shape[0]<1000:
                continue
            df=df.sample(n=1000)

            if file.endswith(".parquet"):
                if "green" in file:
                    df_green_list.append(df)
                else:
                    df_yellow_list.append(df)
        df_green=pd.concat(df_green_list)
        df_yellow=pd.concat(df_yellow_list)
        df_green.to_parquet(os.path.join(streamming_path,"green_stream.parquet"))
        df_yellow.to_parquet(os.path.join(streamming_path,"yellow_stream.parquet"))
    system_maintenance_task>>download_nyc_data_yellow()>>download_nyc_data_green()>>create_streamming_data()