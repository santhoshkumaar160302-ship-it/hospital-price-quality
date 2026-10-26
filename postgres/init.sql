-- One Postgres instance, two databases:
--   airflow   - Airflow's metadata (DAG runs, task state)
--   warehouse - the data warehouse (raw, staging, intermediate, marts, snapshots)
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse OWNER warehouse;
