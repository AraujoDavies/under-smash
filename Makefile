dev:
	poetry run ipython -i code/main.py

run:
	poetry run python code/main.py

db:
	poetry run python code/helper_db.py

.PHONY: down
down:
	docker compose down

up:
	@make down
	docker compose up -d

.PHONY: logs
logs:
	docker compose logs --follow

requirements:
	poetry export -o requirements.txt --without-hashes