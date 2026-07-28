.PHONY: all install format lint test coverage run setup-db seed dbt demo-db dashboard docker

all: lint format test

install:
	pip install -r requirements.txt

format:
	ruff format .

lint:
	ruff check .

test:
	PREFECT_API_URL="" PYTHONPATH=. python -m pytest

coverage:
	PREFECT_API_URL="" PYTHONPATH=. python -m pytest \
		--cov=ingest --cov=warehouse --cov=viz --cov=pipeline --cov=validators \
		--cov-report=term-missing

run:
	python pipeline.py

setup-db:
	PYTHONPATH=. python scripts/setup_db.py

# Synthetic epochs, so the models can be built without downloading the real
# recordings. Includes a daytime nap to exercise sleep-period detection.
seed:
	PYTHONPATH=. python scripts/seed_dev_data.py

dbt:
	dbt deps --profiles-dir .
	dbt build --profiles-dir . --target dev_duckdb

dashboard:
	streamlit run viz/dashboard.py

docker:
	docker build -t sleep-edf-pipeline .

# Rebuild the demo database that backs the Streamlit deployment. Builds into a
# fresh file rather than in place: DuckDB does not reclaim pages on rebuild, so
# repeatedly rebuilding the tracked file grows it every time.
demo-db:
	@test -f data/sleep_data.db || { echo "data/sleep_data.db missing; run the pipeline first"; exit 1; }
	@rm -rf .demo-rebuild && mkdir -p .demo-rebuild
	@python -c "import duckdb; d = duckdb.connect('.demo-rebuild/sleep_data.db'); \
d.execute(\"attach 'data/sleep_data.db' as old (read_only)\"); \
[d.execute(f'create table {t} as select * from old.{t}') for t in ('SLEEP_EPOCHS', 'INGESTION_ERRORS')]; \
d.execute('detach old'); d.close()"
	dbt deps --profiles-dir .
	DB_PATH=.demo-rebuild/sleep_data.db dbt build --profiles-dir . --target dev_duckdb
	@mv .demo-rebuild/sleep_data.db data/sleep_data.db && rm -rf .demo-rebuild
	@echo "Rebuilt data/sleep_data.db ($$(du -h data/sleep_data.db | cut -f1))"
