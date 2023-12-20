import pandas as pd 
from sqlalchemy import create_engine
import dotenv
import os 
dotenv.load_dotenv(".env")


def main():
    #df = pd.read_csv('yellow_tripdata_2021-01.csv',parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])
    user=os.getenv("POSTGRES_USER")
    password=os.getenv("POSTGRES_PASSWORD")
    host=os.getenv("POSTGRES_HOST")
    db_name=os.getenv("POSTGRES_DB")
    print(user,password,host,db_name)
    engine = create_engine('postgresql://'+user+':'+password+'@'+host+':5432/'+db_name)
    year=2021
    df=pd.read_csv("yellow_tripdata_2021-01.csv")
    print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    # for file in os.listdir("data/"+str(year)):
    #     if file.startswith("yellow"):
    #         print(file)
    #         df=pd.read_parquet("data/"+str(year)+"/"+file)
    #         print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine)) 
    #         print(df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index=False,chunksize=10000))
    
    # #remove table if exists
    # engine.execute('DROP TABLE IF EXISTS yellow_taxi_data')
    # engine.execute('DROP TABLE IF EXISTS yellow_taxi_data_test1')
    # engine.execute('DROP TABLE IF EXISTS yellow_taxi_data_test')

if __name__ == "__main__":
    main()