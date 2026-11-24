from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Ops agent: diagnoses failed tasks with Claude. Optional - the pipeline runs unchanged without it.
try:
    import sys
    sys.path.insert(0, "/opt/airflow")
    from agent.airflow_callback import on_failure_diagnose as _ops_agent_callback
except Exception:
    _ops_agent_callback = None

default_args = {
    'owner': 'akash',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
if _ops_agent_callback is not None:
    default_args['on_failure_callback'] = _ops_agent_callback

with DAG(
    dag_id = 'footyflow_pipeline',
    description="Produce match events -> consume to Postgres -> transform with dbt",
    default_args = default_args,
    start_date = datetime(2026,7,2),
    schedule = '@hourly',
    catchup = False,
    tags = ['footyflow', 'kafka', 'dbt'],
) as dag:
    
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='KAFKA_BROKER=kafka:29092 FOOTBALL_API_KEY=$FOOTBALL_API_KEY python /opt/airflow/producer/producer.py',
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='KAFKA_BROKER=kafka:29092 PG_HOST=postgres PG_PORT=5432 timeout 15 python /opt/airflow/consumer/consumer.py || true',
    )


    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command = 'cd /opt/airflow/transform && dbt run --profiles-dir /opt/airflow/transform',

    )

    dbt_test = BashOperator(
        task_id = 'dbt_test',
        bash_command = 'cd /opt/airflow/transform && dbt test --profiles-dir /opt/airflow/transform',
    )

    run_producer >> run_consumer >> dbt_run >> dbt_test 