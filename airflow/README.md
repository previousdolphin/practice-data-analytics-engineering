# Airflow Quickstart

## Local Dev
- `pip install apache-airflow==2.*` and use `airflow standalone` for sandbox.
- Set `AIRFLOW_HOME` to this repo's `airflow/` folder for convenience.

## Jesse: Done!

## Challenges (see challenges.md)
1) Orchestrate daily ETL: ingest -> validate -> transform -> publish.
2) Add a backfill DAG that reprocesses last 3 days on demand.
3) Implement a sensor that waits for sample_data availability.
4) Add data quality task that fails DAG if null% > threshold.
5) Implement a cost guard (skip heavy task if estimated scan > limit).

Use `dags/sample_dag.py` as the starting template.
