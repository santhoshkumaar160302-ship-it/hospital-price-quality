-- Drop everything the pipeline created in the warehouse (keeps the database).
DROP SCHEMA IF EXISTS raw, staging, intermediate, marts, snapshots, analytics CASCADE;
