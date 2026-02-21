import sys
from datetime import date

from pyspark.sql import SparkSession

from lib.config_loader import load_config
from lib.silver_online_retail import transform_online_retail_to_silver, log_count


def build_spark() -> SparkSession:
    """
    Creates Spark session connected to your Spark master container.
    """
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("silver_transform_online_retail")
        .getOrCreate()
    )


def main() -> None:
    # 1) ingest_date from Airflow argument {{ ds }} OR today
    ingest_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())

    # 2) Load config from YAML (config-driven pipeline)
    cfg = load_config()

    # 3) Build spark session
    spark = build_spark()

    # 4) Build paths using config (no hardcoded)
    bucket = cfg["minio"]["bucket"]

    bronze_base = cfg["bronze"]["base_path"]
    bronze_dataset = cfg["bronze"]["dataset_name"]

    silver_base = cfg["silver"]["base_path"]
    silver_dataset = cfg["silver"]["dataset_name"]

    bronze_path = f"s3a://{bucket}/{bronze_base}/{bronze_dataset}/ingest_date={ingest_date}/"
    silver_path = f"s3a://{bucket}/{silver_base}/{silver_dataset}/ingest_date={ingest_date}/"

    # 5) Read bronze
    df_bronze = spark.read.parquet(bronze_path)
    log_count("read_bronze", df_bronze, ingest_date)

    # 6) Transform to true Silver
    df_silver = transform_online_retail_to_silver(df_bronze, cfg, ingest_date)

    # 7) Write silver (overwrite = idempotent)
    (
        df_silver.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f'{{"layer":"silver","stage":"write_done","ingest_date":"{ingest_date}","path":"{silver_path}"}}')

    spark.stop()


if __name__ == "__main__":
    main()
