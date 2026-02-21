import pytest
from pyspark.sql import SparkSession

from jobs.lib.silver_online_retail import transform_online_retail_to_silver


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("silver-unit-tests")
        .getOrCreate()
    )


def test_adds_total_price_and_ingest_date(spark):
    cfg = {
        "silver": {
            "rules": {
                "drop_if_null": ["InvoiceNo", "StockCode", "InvoiceDate", "Country"],
                "numeric_constraints": {"Quantity": {"gt": 0}, "UnitPrice": {"gte": 0}},
            },
            "dedup_keys": ["InvoiceNo", "StockCode", "InvoiceDate", "CustomerID", "Quantity", "UnitPrice"],
        }
    }

    data = [
        {
            "InvoiceNo": "10000",
            "StockCode": "S1",
            "Description": "T-Shirt",
            "Quantity": "2",
            "InvoiceDate": "12/1/2010 8:26",
            "UnitPrice": "10.5",
            "CustomerID": "C1",
            "Country": "Finland",
        }
    ]

    df = spark.createDataFrame(data)
    out = transform_online_retail_to_silver(df, cfg, "2026-02-19")

    row = out.collect()[0]
    assert row["total_price"] == pytest.approx(21.0)
    assert str(row["ingest_date"]) == "2026-02-19"


def test_filters_bad_rows(spark):
    cfg = {
        "silver": {
            "rules": {
                "drop_if_null": ["InvoiceNo", "StockCode", "InvoiceDate", "Country"],
                "numeric_constraints": {"Quantity": {"gt": 0}, "UnitPrice": {"gte": 0}},
            },
            "dedup_keys": ["InvoiceNo", "StockCode", "InvoiceDate", "CustomerID", "Quantity", "UnitPrice"],
        }
    }

    data = [
        {"InvoiceNo": "1", "StockCode": "S1", "Quantity": "0", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "5", "CustomerID": "C1", "Country": "Finland"},
        {"InvoiceNo": "2", "StockCode": "S2", "Quantity": "1", "InvoiceDate": None, "UnitPrice": "5", "CustomerID": "C2", "Country": "Finland"},
        {"InvoiceNo": "3", "StockCode": "S3", "Quantity": "1", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "5", "CustomerID": "C3", "Country": "Finland"},
    ]

    df = spark.createDataFrame(data)
    out = transform_online_retail_to_silver(df, cfg, "2026-02-19")

    assert out.count() == 1


def test_deduplicates(spark):
    cfg = {
        "silver": {
            "rules": {
                "drop_if_null": ["InvoiceNo", "StockCode", "InvoiceDate", "Country"],
                "numeric_constraints": {"Quantity": {"gt": 0}, "UnitPrice": {"gte": 0}},
            },
            "dedup_keys": ["InvoiceNo", "StockCode", "InvoiceDate", "CustomerID", "Quantity", "UnitPrice"],
        }
    }

    data = [
        {"InvoiceNo": "10", "StockCode": "S1", "Quantity": "1", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "5", "CustomerID": "C1", "Country": "Finland"},
        {"InvoiceNo": "10", "StockCode": "S1", "Quantity": "1", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "5", "CustomerID": "C1", "Country": "Finland"},
    ]

    df = spark.createDataFrame(data)
    out = transform_online_retail_to_silver(df, cfg, "2026-02-19")

    assert out.count() == 1
