from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "crypto-data-lakehouse", master: str = "local[1]", executor_memory: str = "1g") -> SparkSession:
    return (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )