datalake_up:
	docker compose -f datalake/datalake_docker-compose.yaml up -d
datalake_down:
	docker compose -f datalake/datalake_docker-compose.yaml down
streaming_up:
	docker compose -f streaming_processing/kafka/docker-compose.yml up -d
streaming_down:
	docker compose -f streaming_processing/kafka/docker-compose.yml down
	
