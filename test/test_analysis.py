from pyspark.sql import SparkSession

from cell_data_analysis.analysis import (
    get_cells_per_site,
    get_frequency_per_site,
)


def test_get_cells_per_site(spark: SparkSession):
    test_df_gsm = spark.createDataFrame(
        [("Y", 300, 1), ("C", 1800, 3), ("D", 1800, 3)],
        ["cell_id", "freq", "site_id"],
    )
    test_df_umts = spark.createDataFrame(
        [("E", 2100, 3), ("F", 2100, 3)], ["cell_id", "freq", "site_id"]
    )
    test_df_lte = spark.createDataFrame(
        [("I", 700, 4), ("J", 2100, 1)], ["cell_id", "freq", "site_id"]
    )

    assert (
        get_cells_per_site(
            gsm_df=test_df_gsm, umts_df=test_df_umts, lte_df=test_df_lte
        ).count()
        == 3
    )


def test_get_frequency_per_site(spark: SparkSession):
    test_df_gsm = spark.createDataFrame(
        [("Y", 300, 1), ("C", 1800, 3), ("D", 1800, 3)],
        ["cell_id", "frequency_band", "site_id"],
    )
    test_df_umts = spark.createDataFrame(
        [("E", 2100, 3), ("F", 2100, 3)],
        ["cell_id", "frequency_band", "site_id"],
    )
    test_df_lte = spark.createDataFrame(
        [("I", 700, 4), ("J", 2100, 1)],
        ["cell_id", "frequency_band", "site_id"],
    )
    assert (
        get_frequency_per_site(
            gsm_df=test_df_gsm, umts_df=test_df_umts, lte_df=test_df_lte
        ).count()
        == 3
    )
