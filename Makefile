all: lint format test

install:
	pip install -r requirements.txt

format:
	ruff format .

lint:
	ruff check .

test:
	PYTHONPATH=. python -m pytest

run:
	python pipeline.py

setup-db:
	PYTHONPATH=. python scripts/setup_db.py
