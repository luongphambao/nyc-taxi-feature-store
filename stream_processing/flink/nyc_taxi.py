from datetime import datetime
from decimal import Decimal


class NYC_Taxi:
    def __init__(self, arr):
        self.list_columns = [
            "vendorid",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "ratecodeid",
            "store_and_fwd_flag",
            "pulocationid",
            "dolocationid",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]
        # list_columns = ['vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        self.vendorid = arr[0]
        # self.tpep_pickup_datetime=datetime.strptime(arr[1],"%Y-%m-%d %H:%M:%S")
        # self.tpep_dropoff_datetime=datetime.strptime(arr[2],"%Y-%m-%d %H:%M:%S")
        # convert Timestamp to string
        self.tpep_pickup_datetime = str(arr[1])
        self.tpep_dropoff_datetime = str(arr[2])
        self.passenger_count = arr[3]
        self.trip_distance = arr[4]
        self.ratecodeid = arr[5]
        self.store_and_fwd_flag = arr[6]
        self.pulocationid = arr[7]
        self.dolocationid = arr[8]
        self.payment_type = arr[9]
        self.fare_amount = arr[10]
        self.extra = arr[11]
        self.mta_tax = arr[12]
        self.tip_amount = arr[13]
        self.tolls_amount = arr[14]
        self.improvement_surcharge = arr[15]
        self.total_amount = arr[16]
        self.congestion_surcharge = arr[17]
        self.airport_fee = arr[18]

    def data(self):
        return [
            self.vendorid,
            self.tpep_pickup_datetime,
            self.tpep_dropoff_datetime,
            self.passenger_count,
            self.trip_distance,
            self.ratecodeid,
            self.store_and_fwd_flag,
            self.pulocationid,
            self.dolocationid,
            self.payment_type,
            self.fare_amount,
            self.extra,
            self.mta_tax,
            self.tip_amount,
            self.tolls_amount,
            self.improvement_surcharge,
            self.total_amount,
            self.congestion_surcharge,
        ]


import pandas as pd

df = pd.read_parquet(
    "/home/baolp/mlops/module2/MLE2/data/2021/yellow_tripdata_2021-01.parquet"
)
row_arr = df.iloc[0].values
print(row_arr)
columns = df.columns
columns = [col.lower() for col in columns]
print(columns)
nyctaxi = NYC_Taxi(row_arr)
print(nyctaxi.data())
# print(df.columns)
