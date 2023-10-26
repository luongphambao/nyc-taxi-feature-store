import os 
import pandas as pd 

data_sample="data/2022/green_tripdata_2022-01.parquet"
df=pd.read_parquet(data_sample)
print(df.head())
print(df.describe())
