from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime



default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "depends_on_past": False
}
with DAG(
    dag_id="spark_minio_medallion_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False
) as dag:
    
    # 1. Setup: Upload Local Data to MinIO Raw
    ingestion = BashOperator(
        task_id="file_setup",
        env={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": "http://10.10.12.3:9001"
        },
        bash_command="""
            aws --endpoint-url $AWS_ENDPOINT_URL s3 cp /home/anhtn/data/yellow_tripdata_2025.parquet s3://bronze/raw/yellow_tripdata_2025.parquet
            aws --endpoint-url $AWS_ENDPOINT_URL s3 cp /home/anhtn/data/taxi_zone_lookup.csv s3://bronze/raw/taxi_zone_lookup.csv
        """
    )

    # 2. Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/anhtn/nathai_scripts/bronze_to_silver.py"
    )

    # 3. Silver to Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/anhtn/nathai_scripts/silver_to_gold.py"
    )

    # 4. Validation/Reading tasks
    read_bronze = BashOperator(
        task_id="read_bronze",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/anhtn/nathai_scripts/read_bronze.py"
    )

    learning_gold = BashOperator(
        task_id="learning_gold",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/anhtn/nathai_scripts/learning_gold.py"
    )

    ingestion >> bronze_to_silver >> silver_to_gold >> read_bronze >> learning_gold
   #download_data >> create_bronze_dir >> upload_to_bronze >> bronze >> silver >> gold