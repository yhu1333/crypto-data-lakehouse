from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "crypto-data-lakehouse",
    master: str = "local[*]",
    driver_memory: str = "4g",
    shuffle_partitions: str = "16",
) -> SparkSession:
    return (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )