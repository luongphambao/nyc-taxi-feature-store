## How-to guide

1. Create and install `conda` environment

    ```shell
    conda create -n data-validation python=3.9
    conda activate data-validation
    pip install -r requirements.txt
    ```

2. Start the docker compose

    ```shell
    docker compose -f docker-compose.yaml
    ```

3. Ingest a parquet file to PostgreSQL
    ```shell
    python utils/initialize_db.py
    ```
4. Profile data with `Deequ`
    ```
    python scripts/data_profiling.py
    ```
5. Data validation with `GX`:
    Please check the notebooks in the `notebooks/` folder.

## Data

The data is taken from [this website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)