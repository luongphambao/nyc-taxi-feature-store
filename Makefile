datalake_up:
	docker compose -f datalake/minio_docker-compose.yml up -d
datalake_down:
	docker compose -f datalake/minio_docker-compose.yml down
datalake_restart:
	docker compose -f datalake/minio_docker-compose.yml down
	docker compose -f datalake/minio_docker-compose.yml up -d
airflow_up:
	docker compose -f airflow-docker-compose.yaml up -d
kafka_up:
	docker compose -f stream_processing/kafka/docker-compose.yml up -d
kafka_down:
	docker compose -f stream_processing/kafka/docker-compose.yml down
# streamming_up:
# 	docker compose -f stream_processing/docker-compose.yaml build 
# 	docker compose -f stream_processing/docker-compose.yaml up -d
streamming_down:
	docker compose -f stream_processing/docker-compose.yaml down
monitoring_up:
	docker compose -f monitoring/prom-graf-docker-compose.yaml up -d
monitoring_down:
	docker compose -f monitoring/prom-graf-docker-compose.yaml down
monitoring__restart:
	docker compose -f monitoring/prom-graf-docker-compose.yaml restart
elk_up:
	cd monitoring/elk  && docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
warehouse_up:
	docker compose -f postgresql-docker-compose.yaml up -d
run_all:
	docker compose -f docker-compose.yml up -d
	docker compose -f airflow-docker-compose.yaml up -d
	docker compose -f monitoring/prom-graf-docker-compose.yaml up -d
	cd monitoring/elk  && docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d

run_data :
	docker compose -f docker-compose.yml up -d