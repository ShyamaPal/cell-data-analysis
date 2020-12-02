import pytest
from pyspark.sql import SparkSession

from cell_data_analysis.connection import get_spark_connection


@pytest.fixture(name="spark")
def test_spark_con():
    return get_spark_connection(app_name="testApp")


@pytest.fixture(name="df")
def test_get_df(spark: SparkSession):
    return spark.createDataFrame(
        [("a", 1), ("a", 2), ("c", 3)], ["Col1", "Col2"]
    )
