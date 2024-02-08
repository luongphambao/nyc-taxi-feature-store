bash run.sh register_connector configs/postgresql-cdc.json
python3 create_table.py
python3 insert_table.py