
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="uber_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_kafka = BashOperator(
        task_id="start_kafka_producer",
        bash_command="python kafka/producer.py"
    )

    run_spark = BashOperator(
        task_id="run_spark_stream",
        bash_command="python spark/streaming_job.py"
    )

    run_kafka >> run_spark
