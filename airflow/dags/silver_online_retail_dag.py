from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}


with DAG(
    dag_id="online_retail_pipeline",
    start_date=datetime(2026, 2, 16),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "spark"],
) as dag:

    bronze_ingest = SparkSubmitOperator(
        task_id="bronze_ingest",
        conn_id="spark_default",
        application="/opt/spark/jobs/bronze_ingest.py",
        name="bronze-online-retail",
        verbose=True,
        application_args=["{{ ds }}"],
        conf=S3A_CONF,
    )

    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        conn_id="spark_default",
        application="/opt/spark/jobs/silver_transform.py",
        name="silver-online-retail",
        verbose=True,
        application_args=["{{ ds }}"],
        conf=S3A_CONF,
    )
    
    validate_silver = SparkSubmitOperator(
    task_id="validate_silver",
    conn_id="spark_default",
    application="/opt/spark/jobs/validate_silver_ge.py",
    name="validate_silver_online_retail",
    verbose=True,
    application_args=["{{ ds }}"],
    conf=S3A_CONF,
)

    bronze_ingest >> silver_transform >> validate_silver

