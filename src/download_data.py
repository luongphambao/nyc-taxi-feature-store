import argparse
import os
import numpy as np
import pandas as pd
import requests

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir", type=str, default="data", help="directory to save data"
    )
    parser.add_argument(
        "--data_type", type=str, default="green_tripdata_", help="data type"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    data_dir = args.data_dir
    data_type = args.data_type
    url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    years = ["2020"]
    for year in years:
        year_path = os.path.join(data_dir, year)
        if not os.path.exists(year_path):
            os.makedirs(year_path)
        for month in months:
            url_download = url_prefix + data_type + year + "-" + month + ".parquet"
            print(url_download)
            file_path = os.path.join(
                year_path, data_type + year + "-" + month + ".parquet"
            )
            try:
                r = requests.get(url_download, allow_redirects=True)
                open(file_path, "wb").write(r.content)
            except:
                print("Error in downloading file: " + url_download)
                continue
