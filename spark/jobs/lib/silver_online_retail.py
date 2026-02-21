from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def log_count(stage: str, df: DataFrame, ingest_date: str) -> None:
    """
    Structured logging (JSON-like). Easy to read in logs and searchable.
    """
    print(
        f'{{"layer":"silver","stage":"{stage}","ingest_date":"{ingest_date}","rows":{df.count()}}}'
    )


def transform_online_retail_to_silver(df: DataFrame, cfg: Dict[str, Any], ingest_date: str) -> DataFrame:
    """
    Turns Bronze online_retail into true Silver:
    - Add ingest_date column
    - Cast types
    - Parse dates
    - Apply rules
    - Deduplicate
    - Add analytics columns
    """
    rules = cfg["silver"]["rules"]
    dedup_keys = cfg["silver"]["dedup_keys"]

    # 1) Add ingest_date column (because Bronze stored it only in folder name)
    df = df.withColumn("ingest_date", F.to_date(F.lit(ingest_date)))

    # 2) Parse InvoiceDate into a real timestamp
    # Common Online Retail format: "12/1/2010 8:26"
    df = df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))

    # 3) Cast numeric columns into correct types
    df = df.withColumn("Quantity", F.col("Quantity").cast("int"))
    df = df.withColumn("UnitPrice", F.col("UnitPrice").cast("double"))

    # 4) Add analytics column total_price
    df = df.withColumn("total_price", (F.col("Quantity") * F.col("UnitPrice")).cast("double"))

    # Logging after basic standardization
    log_count("after_standardize", df, ingest_date)

    # 5) Drop rows with nulls in required columns
    for col_name in rules["drop_if_null"]:
        df = df.filter(F.col(col_name).isNotNull())

    log_count("after_drop_nulls", df, ingest_date)

    # 6) Apply numeric constraints
    # Quantity > 0
    if "Quantity" in rules["numeric_constraints"]:
        gt_value = rules["numeric_constraints"]["Quantity"]["gt"]
        df = df.filter(F.col("Quantity") > F.lit(gt_value))

    # UnitPrice >= 0
    if "UnitPrice" in rules["numeric_constraints"]:
        gte_value = rules["numeric_constraints"]["UnitPrice"]["gte"]
        df = df.filter(F.col("UnitPrice") >= F.lit(gte_value))

    log_count("after_numeric_rules", df, ingest_date)

    # 7) Deduplicate
    df = df.dropDuplicates(dedup_keys)

    log_count("after_dedup", df, ingest_date)

    return df
