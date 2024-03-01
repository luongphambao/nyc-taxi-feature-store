import os

import pandas as pd

data_dir = "data/2020"
data_type1 = "green_tripdata_"
data_type2 = "yellow_tripdata_"
root = "data/"
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

list_parquet1 = [
    os.path.join(data_dir, data_type1 + "2020-" + month + ".parquet")
    for month in months
]
list_parquet2 = [
    os.path.join(data_dir, data_type2 + "2020-" + month + ".parquet")
    for month in months
]
list_df1 = [pd.read_parquet(parquet) for parquet in list_parquet1]
list_df2 = [pd.read_parquet(parquet) for parquet in list_parquet2]
df1 = pd.concat(list_df1)
df2 = pd.concat(list_df2)
df1.to_parquet(root + "/green_tripdata_2020.parquet")
df2.to_parquet(root + "/yellow_tripdata_2020.parquet")
# check missing values
print(df1.isnull().sum())
