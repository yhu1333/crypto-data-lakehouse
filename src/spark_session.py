from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("crypto-data-lakehouse")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )