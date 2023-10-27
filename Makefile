datalake_up:
	docker compose -f datalake/minio_docker-compose.yml up -d
datalake_down:
	docker compose -f datalake/minio_docker-compose.yml down
airflow_up:
	docker compose -f airflow/postgresql-docker-compose.yaml up -d
	docker compose -f airflow/airflow-docker-compose.yaml up -d