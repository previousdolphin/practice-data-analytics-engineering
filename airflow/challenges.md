# Airflow Challenges

- Build a DAG with @daily schedule and task groups: ingest -> dq -> transform -> publish.
- Add a manual trigger param `reprocess_days` to backfill recent partitions.
- Use XCom to pass the computed partition date into downstream tasks.
- Add a `ShortCircuitOperator` to skip heavy tasks if cost estimate too high.
- Emit lineage metadata to a JSON file at the end of the DAG.
