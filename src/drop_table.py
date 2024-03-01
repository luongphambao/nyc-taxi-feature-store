import os
from glob import glob

import pandas as pd
from dotenv import load_dotenv

from postgresql_client import PostgresSQLClient

load_dotenv()


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Create devices table
    drop_table_query = """
        DROP TABLE IF EXISTS public.nyc_taxi;
    """
    try:
        pc.execute_query(drop_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
