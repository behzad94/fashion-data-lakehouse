import argparse
import logging
import yaml
from pyspark.sql import SparkSession


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def main():
    # -----------------------------
    # 1. Parse arguments
    # -----------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--ds", required=True, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    # -----------------------------
    # 2. Logging setup
    # -----------------------------
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("gold_marts")

    logger.info("Starting Gold Marts Job")
    logger.info(f"Config path: {args.config}")
    logger.info(f"Execution date (ds): {args.ds}")

    # -----------------------------
    # 3. Load config
    # -----------------------------
    config = load_config(args.config)

    bucket = config["minio"]["bucket"]

    silver_base = config["silver"]["base_path"]
    silver_dataset = config["silver"]["dataset_name"]

    gold_base = config["gold"]["base_path"]
    gold_dataset = config["gold"]["dataset_name"]

    # -----------------------------
    # 4. Build paths
    # -----------------------------
    silver_input_path = (
        f"s3a://{bucket}/"
        f"{silver_base}/"
        f"{silver_dataset}/"
        f"ingest_date={args.ds}/"
    )

    gold_output_base_path = (
        f"s3a://{bucket}/"
        f"{gold_base}/"
        f"{gold_dataset}/"
        f"ds={args.ds}/"
    )

    logger.info(f"Silver input path: {silver_input_path}")
    logger.info(f"Gold output base path: {gold_output_base_path}")

    # -----------------------------
    # 5. Start Spark
    # -----------------------------
    spark = build_spark_session("gold-marts")

    # -----------------------------
    # 6. Read Silver Data
    # -----------------------------
    logger.info("Reading Silver data...")
    df = spark.read.parquet(silver_input_path)

    record_count = df.count()
    logger.info(f"Silver record count: {record_count}")

    logger.info("Gold skeleton job completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()