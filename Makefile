run:
	poetry run python main.py

.PHONY: down
down:
	docker compose down

up:
	@make down
	docker compose up -d

logs:
	docker compose logs --follow

requirements:
	poetry export -o requirements.txt --without-hashes