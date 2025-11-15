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
    schedule_interval=None, # Chạy mỗi ngày một lần
    catchup=False, # Không chạy lại các lượt chạy DAG trong quá khứ khi DAG được kích hoạt lần đầu
    tags=['crypto', 'batch', 'spark'], # Thêm tags để dễ quản lý trong Airflow UI
) as dag:

    run_yahoo_producer = BashOperator(
        task_id="run_yahoo_producer",
        bash_command="""
        echo "[AIRFLOW] Starting Yahoo Finance Data Producer..." &&
        python3 /opt/airflow/scripts/yahoo_producer.py
        """,
    )


    run_daily_pipeline = BashOperator(
        task_id="run_daily_data_pipeline",
        bash_command="""
        echo "[AIRFLOW] Starting Daily Data Pipeline (reading from Kafka, saving to HDFS, training daily model)..." &&
        docker exec spark-master bash -c "
            export PATH=$PATH:/opt/spark/bin &&
            spark-submit /opt/spark/work-dir/batch/data_pipeline_1d.py
        "
        """
    )

    run_hourly_pipeline = BashOperator(
        task_id="run_hourly_data_pipeline",
        bash_command="""
        echo "[AIRFLOW] Starting Hourly Data Pipeline (reading from Kafka, saving to HDFS, training hourly model)..." &&
        docker exec spark-master bash -c "
            export PATH=$PATH:/opt/spark/bin &&
            spark-submit /opt/spark/work-dir/batch/data_pipeline_1h.py
        "
        """
    )

    write_to_mongo = BashOperator(
        task_id="write_results_to_mongodb",
        bash_command="""
        echo "[AIRFLOW] Starting Write to MongoDB Pipeline..." &&
        docker exec spark-master bash -c "
            export PATH=$PATH:/opt/spark/bin &&
            spark-submit /opt/spark/work-dir/batch/write_to_mongo.py
        "
        """
    )

    run_yahoo_producer >> run_hourly_pipeline >> run_daily_pipeline >> write_to_mongo