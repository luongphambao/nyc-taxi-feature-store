import argparse
import json
import os
import random
import shutil

import numpy as np
import pandas as pd


def type_to_avro_type(dtype):
    if dtype == "int64":
        return "int"
    elif dtype == "float64":
        return "float"
    elif dtype == "object" or dtype == "datetime64[ns]":
        return "string"


def main(args):
    # Clean up the avro schema folder if exists
    if os.path.exists(args["schema_folder"]):
        shutil.rmtree(args["schema_folder"])

    os.mkdir(args["schema_folder"])
    columns_list = [
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
    ]
    df_sample = pd.read_parquet("streamming_data.parquet")
    print(len(df_sample.columns))
    print(len(columns_list))
    type_list = [df_sample[col].dtype for col in df_sample.columns]
    print(type_list)
    for schema_idx in range(args["num_schemas"]):
        # Initialize schema template
        schema = {
            "doc": "Sample schema to help you get started.",
            "fields": [
                {"name": "nyc_taxi_id", "type": "int"},
                {"name": "created", "type": "string"},
            ],
            "name": "nyctaxi",
            "namespace": "example.avro",
            "type": "record",
        }
        for feature_idx in range(len(columns_list)):
            print(type_list[feature_idx])

            schema["fields"].append(
                {
                    "name": columns_list[feature_idx],
                    "type": type_to_avro_type(str(type_list[feature_idx])),
                }
            )

        # Write this schema to the Avro output folder
        print(schema)
        with open(f'{args["schema_folder"]}/schema_{schema_idx}.avsc', "w+") as f:
            json.dump(schema, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_schemas",
        default=1,
        type=int,
        help="Number of avro schemas to generate.",
    )
    parser.add_argument(
        "-m",
        "--min_features",
        default=8,
        type=int,
        help="Minumum number of features for each device",
    )
    parser.add_argument(
        "-a",
        "--max_features",
        default=10,
        type=int,
        help="Maximum number of features for each device",
    )
    parser.add_argument(
        "-o",
        "--schema_folder",
        default="./avro_schemas",
        help="Folder containing all generated avro schemas",
    )
    args = vars(parser.parse_args())
    main(args)
