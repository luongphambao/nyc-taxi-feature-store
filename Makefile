datalake_up:
	docker compose -f datalake/minio_docker-compose.yml up -d
datalake_down:
	docker compose -f datalake/minio_docker-compose.yml down
datalake_restart:
	docker compose -f datalake/minio_docker-compose.yml down
	docker compose -f datalake/minio_docker-compose.yml up -d
airflow_up:
	docker compose -f airflow/postgresql-docker-compose.yaml up -d
	docker compose -f airflow/airflow-docker-compose.yaml up -d
spark_up:
	docker compose -f spark/docker-compose.yaml up -d
spark_down:
	docker compose -f spark/docker-compose.yaml down
kafka_up:
	docker compose -f kafka/docker-compose.yml up -d
kafka_down:
	docker compose -f kafka/docker-compose.yml down
validation_up:
	docker compose -f data-validation/docker-compose.yaml up -d
validation_down:
	docker compose -f data-validation/docker-compose.yaml down
streamming_up:
	docker compose -f stream_processing/docker-compose.yaml build 
	docker compose -f stream_processing/docker-compose.yaml up -d
streamming_down:
	docker compose -f stream_processing/docker-compose.yaml down
monitoring_up:
	docker compose -f monitoring/prom-graf-docker-compose.yaml up -d
monitoring_down:
	docker compose -f monitoring/prom-graf-docker-compose.yaml down
elk_up:
	cd monitoring/elk  && docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
db_up:
	docker compose -f postgresql-docker-compose.yaml up -d