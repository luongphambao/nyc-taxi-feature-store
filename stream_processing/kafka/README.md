```shell
docker compose -f docker-compose.yml up -d
```

```shell
bash streaming_demo/run.sh register_connector configs/postgresql-cdc.json
```

```shell
# Create an empty table in PostgreSQL
python streaming_demo/utils/create_table.py
# Periodically insert a new record to the table
python streaming_demo/utils/insert_table.py
```