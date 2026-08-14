.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv

help:
	@echo "queuefy development commands"
	@echo "  make install    create the virtualenv and install the package with its development tools"
	@echo "  make servers    start the redis, mysql and postgres the full suite needs"
	@echo "  make servers-stop  remove those same containers"
	@echo "  make test       run the suite"
	@echo "  make coverage   run the suite with the 100% branch coverage gate"
	@echo "  make stress     run many machines against every server that answers"
	@echo "  make lint       check the code"
	@echo "  make format     format the code"
	@echo "  make build      build the wheel and the sdist"
	@echo "  make clean      remove build and coverage artifacts"

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip
	# the mysql and postgres drivers belong here and not in the extras: the suite reaches a store whenever its port answers, so with `make servers` up and a driver missing every test of that store errors on the engine instead of being left out. cryptography is what aiomysql authenticates to mysql 8 with
	$(VENV)/bin/python -m pip install -e ".[sqlalchemy,redis]" pytest pytest-asyncio pytest-cov pytest-timeout ruff black aiosqlite aiomysql asyncpg cryptography build

servers:
	docker run -d --name queuefy-redis -p 6399:6379 redis:7-alpine
	docker run -d --name queuefy-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=queuefy -p 3399:3306 mysql:8.4
	docker run -d --name queuefy-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queuefy -p 5499:5432 postgres:16-alpine

servers-stop:
	docker rm -f queuefy-redis queuefy-mysql queuefy-postgres

test:
	$(VENV)/bin/python -m pytest

coverage:
	$(VENV)/bin/python -m pytest --cov

stress:
	$(VENV)/bin/python -m pytest -m stress -v

lint:
	$(VENV)/bin/python -m ruff check .
	$(VENV)/bin/python -m black --check .

format:
	$(VENV)/bin/python -m ruff check --fix .
	$(VENV)/bin/python -m black .

build:
	$(VENV)/bin/python -m build

clean:
	rm -rf dist build htmlcov .coverage coverage.xml .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
