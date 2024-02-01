from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from pendulum import datetime

POSTGRES_CONN_ID = "postgres_default"


with DAG(dag_id="gx", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE strawberries (
                id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100),
                amount INT
            );

            INSERT INTO strawberries (id, name, amount)
            VALUES ('001', 'Strawberry Order 1', 10),
                ('002', 'Strawberry Order 2', 5),
                ('003', 'Strawberry Order 3', 8),
                ('004', 'Strawberry Order 4', 3),
                ('005', 'Strawberry Order 5', 12);
            """,
    )

    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="public.strawberries",
        database="k6",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )

    drop_table_pg = PostgresOperator(
        task_id="drop_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE strawberries;
            """,
    )

    create_table_pg >> gx_validate_pg >> drop_table_pg
