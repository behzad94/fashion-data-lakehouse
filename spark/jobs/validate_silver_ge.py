import json
import os
import sys
from datetime import date

import great_expectations as ge
import yaml
from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("validate_silver_online_retail")
        .getOrCreate()
    )


def load_config() -> dict:
    config_path = os.environ.get("FDP_CONFIG", "/opt/fdp/config/dev.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_suite(suite_name: str) -> dict:
    suite_path = f"/opt/fdp/great_expectations/expectations/{suite_name}.json"
    with open(suite_path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    ingest_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())

    cfg = load_config()
    spark = build_spark()

    bucket = cfg["minio"]["bucket"]
    silver_base = cfg["silver"]["base_path"]
    silver_dataset = cfg["silver"]["dataset_name"]

    quality_base = cfg["quality"]["base_path"]
    quality_dataset = cfg["quality"]["dataset_name"]
    suite_name = cfg["quality"]["suite_name"]

    # 1) Read Silver partition for this ingest_date
    silver_path = f"s3a://{bucket}/{silver_base}/{silver_dataset}/ingest_date={ingest_date}/"
    df = spark.read.parquet(silver_path)

    # 2) Convert Spark DataFrame into Great Expectations dataset wrapper
    ge_df = ge.dataset.SparkDFDataset(df)

    # 3) Load suite JSON and apply expectations
    suite = load_suite(suite_name)

    validation_results = []
    success = True

    for exp in suite["expectations"]:
        exp_type = exp["expectation_type"]
        kwargs = exp["kwargs"]

        # Run expectation by calling method name dynamically
        # Example: ge_df.expect_column_values_to_not_be_null(column="InvoiceNo")
        method = getattr(ge_df, exp_type)
        result = method(**kwargs)

        validation_results.append(
            {
                "expectation_type": exp_type,
                "kwargs": kwargs,
                "success": result["success"]
            }
        )

        if result["success"] is False:
            success = False

    # 4) Write validation report to MinIO (JSON)
    report = {
        "dataset": quality_dataset,
        "ingest_date": ingest_date,
        "suite_name": suite_name,
        "success": success,
        "results": validation_results
    }

    report_json = json.dumps(report, indent=2)

    # We write a small JSON file via Spark parallelize -> text
    # This avoids needing boto3 and keeps it in Spark world.
    report_path = f"s3a://{bucket}/{quality_base}/{quality_dataset}/ingest_date={ingest_date}/validation.json"

    # Write report with overwrite (safe for re-runs)
    spark.createDataFrame([(report_json,)], ["value"]) \
     .coalesce(1) \
     .write \
     .mode("overwrite") \
     .text(report_path)


    print(f'{{"layer":"quality","stage":"write_report","path":"{report_path}","success":{str(success).lower()}}}')

    spark.stop()

    # 5) Fail pipeline if not success
    if not success:
        raise SystemExit("Great Expectations validation FAILED")


if __name__ == "__main__":
    main()
