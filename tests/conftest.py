import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("crypto-lakehouse-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


sys.path.append(str(Path(__file__).resolve().parents[1]))
