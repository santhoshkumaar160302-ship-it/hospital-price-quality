COMPOSE = docker compose
RUN = $(COMPOSE) run --rm --no-deps airflow-scheduler
DBT = /home/airflow/dbt-venv/bin/dbt
PSQL = $(COMPOSE) exec -T postgres psql -U postgres -d warehouse

.PHONY: up down clean ps logs run demo reset dbt docs publish e2e test lint

.env:
	echo "AIRFLOW_UID=$$(id -u)" > .env

up: .env           ## build and start Postgres, Airflow and the dashboard
	mkdir -p landing
	$(COMPOSE) up -d --build

down:              ## stop, keep data
	$(COMPOSE) down

clean:             ## stop and delete all data, downloads and logs
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f airflow-scheduler

run:               ## trigger the real pipeline (downloads live CMS data)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger hospital_price_quality

reset:             ## wipe the warehouse schemas (keeps downloaded files)
	$(PSQL) < scripts/reset_warehouse.sql

demo: reset        ## offline demo on fixture data: two releases, then the dashboard
	$(RUN) bash -c "python -m ingest.cli --source-dir /opt/airflow/fixtures/phase1 --min-rows 1 \
	  && cd dbt && $(DBT) build --profiles-dir . \
	  && cd .. && python -m ingest.cli --source-dir /opt/airflow/fixtures/phase2 --min-rows 1 \
	  && cd dbt && $(DBT) build --profiles-dir ."
	@echo "Open http://localhost:8501  (run 'make reset' before loading real data)"

dbt:               ## run any dbt command: make dbt cmd="test --select marts"
	$(RUN) bash -c "cd dbt && $(DBT) $(cmd) --profiles-dir ."

docs:              ## generate and serve dbt docs (lineage graph) on :8081
	$(COMPOSE) run --rm --no-deps -p 8081:8081 airflow-scheduler bash -c \
	  "cd dbt && $(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir . --port 8081 --host 0.0.0.0"

publish:           ## export the dashboard tables to app/data/*.parquet for the public demo
	mkdir -p app/data
	$(COMPOSE) run --rm --no-deps -v ./app/data:/app/data dashboard python export_data.py

e2e:               ## end-to-end fixture test inside Docker (same as CI)
	$(RUN) bash -c "DBT_BIN=$(DBT) python scripts/e2e_fixtures.py"

test:              ## unit tests (no Docker needed)
	pytest -q

lint:
	ruff check .
