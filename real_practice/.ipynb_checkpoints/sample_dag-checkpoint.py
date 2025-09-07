from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

def ingest(**ctx):
    print("ingest placeholder - read CSVs, write bronze")


def dq(**ctx):
    print("dq placeholder - compute null %, ranges; raise on failure threshold")


def transform(**ctx):
    print("transform placeholder - build curated tables")


def publish(**ctx):
    print("publish placeholder - refresh materialized views / metrics")


def cost_guard(**ctx):
    # pretend to compute bytes scanned; skip if too high
    est_gb = 5
    return est_gb < 20

with DAG(
    dag_id="ae_sample_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ae-practice"]
) as dag:
    t_ingest = PythonOperator(task_id="ingest", python_callable=ingest)
    t_dq = PythonOperator(task_id="dq", python_callable=dq)
    t_guard = ShortCircuitOperator(task_id="cost_guard", python_callable=cost_guard)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_publish = PythonOperator(task_id="publish", python_callable=publish)

    t_ingest >> t_dq >> t_guard >> t_transform >> t_publish
