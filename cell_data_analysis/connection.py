from pyspark.sql import SparkSession


def get_spark_connection(app_name: str) -> SparkSession:
    """
    It creates and returns a spark session
    :param app_name:
    :return: SparkSession
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
