
from decimal import Decimal
from datetime import datetime
class NYC_Taxi:
    def __init__(self,arr):
        self.list_columns = ['vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        #list_columns = ['vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        self.vendorid=arr[0]
        #self.tpep_pickup_datetime=datetime.strptime(arr[1],"%Y-%m-%d %H:%M:%S")
        #self.tpep_dropoff_datetime=datetime.strptime(arr[2],"%Y-%m-%d %H:%M:%S")
        self.tpep_pickup_datetime=arr[1]
        self.tpep_dropoff_datetime=arr[2]
        self.passenger_count=int(arr[3])
        self.trip_distance=Decimal(arr[4])
        self.ratecodeid=int(arr[5])
        self.store_and_fwd_flag=arr[6]
        self.pulocationid=int(arr[7])
        self.dolocationid=int(arr[8])
        self.payment_type=int(arr[9])
        self.fare_amount=Decimal(arr[10])
        self.extra=Decimal(arr[11])
        self.mta_tax=Decimal(arr[12])
        self.tip_amount=Decimal(arr[13])
        self.tolls_amount=Decimal(arr[14])
        self.improvement_surcharge=Decimal(arr[15])
        self.total_amount=Decimal(arr[16])
        self.congestion_surcharge=Decimal(arr[17])
    def data(self):
        return [self.vendorid,self.tpep_pickup_datetime,self.tpep_dropoff_datetime,self.passenger_count,self.trip_distance,self.ratecodeid,self.store_and_fwd_flag,self.pulocationid,self.dolocationid,self.payment_type,self.fare_amount,self.extra,self.mta_tax,self.tip_amount,self.tolls_amount,self.improvement_surcharge,self.total_amount,self.congestion_surcharge]



import pandas as pd 
df=pd.read_parquet("data/2021/yellow_tripdata_2021-01.parquet")
row_arr=df.iloc[0].values
print(row_arr)
columns=df.columns
columns=[col.lower() for col in columns]
print(columns)
nyctaxi=NYC_Taxi(row_arr)
print(nyctaxi.data())
#print(df.columns)