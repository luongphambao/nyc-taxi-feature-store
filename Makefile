datalake_up:
	docker compose -f datalake/minio_docker-compose.yml up -d
datalake_down:
	docker compose -f datalake/minio_docker-compose.yml down
airflow_up:
	docker compose -f airflow/postgresql-docker-compose.yaml up -d
	docker compose -f airflow/airflow-docker-compose.yaml up -d
spark_up:
	docker compose -f spark/docker-compose.yaml up -d
spark_down:
	docker compose -f spark/docker-compose.yaml down
kafka_up:
	docker compose -f kafka/docker-compose.yml up -d
karfka_down:
	docker compose -f kafka/docker-compose.yml down
validation_up:
	docker compose -f data-validation/docker-compose.yaml up -d
validation_down:
	docker compose -f data-validation/docker-compose.yaml down
