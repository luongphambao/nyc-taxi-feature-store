datalakeminio_up:
	docker compose -f service/minio_docker-compose.yml up -d
datalakeminio_down:
	docker compose -f service/minio_docker-compose.yml down
