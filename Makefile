
.PHONY: build up down logs sh fmt lint

build:
	docker compose build

up:
	docker compose up

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

sh:
	docker exec -it streamlit_app bash

fmt:
	docker run --rm -v $(PWD)/src:/code ghcr.io/psf/black:24.10.0 /code

lint:
	docker run --rm -v $(PWD)/src:/code ghcr.io/astral-sh/ruff:latest check /code
