from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "dan",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily", # use @once if you want it to run immediately after it is built
    catchup=False, # use True if you want to run for all missed intervals
) as dag:

    run_batch = BashOperator(
        task_id="run_batch_pipeline",
        bash_command="""
        docker exec spark-master bash -c "
            export PATH=$PATH:/opt/spark/bin &&
            spark-submit /opt/spark/work-dir/batch/data_pipeline_1h.py &&
            spark-submit /opt/spark/work-dir/batch/data_pipeline_1d.py &&
            spark-submit /opt/spark/work-dir/batch/write_to_mongo.py
        "
        """
    )
