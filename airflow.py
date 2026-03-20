from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime(2026, 3, 19),
    "retries": 1
}

with DAG(
    dag_id="spark_hdfs_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # runs daily
    catchup=False
) as dag:

    spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        /home/n3cr0d3m0nncrdmn/1.py
        """
    )

    spark_job

