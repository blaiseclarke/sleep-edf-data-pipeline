install:
	pip install -r requirements.txt

format:
	ruff format .

lint:
	ruff check .

test:
	PYTHONPATH=. pytest

run:
	python pipeline.py
