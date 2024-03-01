import os

import pandas as pd

data_path = "data/final_data/"
for file in os.listdir(data_path):
    if file.startswith("yellow"):
        print(file)
        df = pd.read_parquet(data_path + file)
        print(df.head())
        # #drop airport_free column
        # df=df.drop(columns=['airport_fee'])
        # #drop missing values
        # df=df.dropna()

        # #convert column to lower case
        # df.columns=df.columns.str.lower()
        # df.to_parquet(data_path+file)
