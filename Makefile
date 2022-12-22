.PHONY: docker

docker:
	docker compose -p "discursus-data-platform" --file docker-compose.yml up --build

docker-clean: docker-system-prune docker-volume-prune

docker-system-prune:
	docker system prune --all --force

docker-volume-prune:
	docker volume prune