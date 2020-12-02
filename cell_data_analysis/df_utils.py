from pyspark.sql import DataFrame, functions as sf, SparkSession
from typing import Tuple


def read_csv_data_frame(
    spark: SparkSession,
    parent_path: str,
    delimiter: str = ";",
) -> DataFrame:
    """
    Reads csv data and returns spark dataframe
    :param spark:
    :param parent_path:
    :param delimiter:
    :return:
    """
    return (
        spark.read.format("csv")
        .option("delimiter", delimiter)
        .option("header", "true")
        .load(f"file:///{parent_path}/*/*/*/*")
    )


def write_data_frame(
    df: DataFrame,
    output_path: str,
    partition_cols: Tuple[str] = ("year", "month", "day"),
    write_format: str = "csv",
    write_mode: str = "overwrite",
) -> None:
    """
    Writes dataframe in the given output path
    :param df:
    :param output_path:
    :param partition_cols:
    :param write_format:
    :param write_mode:
    :return:
    """
    df.write.format(write_format).mode(write_mode).option(
        "header", "true"
    ).partitionBy(*partition_cols).save(output_path)


def get_count_per_group(
    cell_df: DataFrame, count_alias: str, group_col: str = "site_id"
) -> DataFrame:
    """
    Calculates cells per technology per site
    :param cell_df:
    :param group_col:
    :param count_alias:
    :return: count_dataframe:
    """
    return (
        cell_df.groupBy(group_col)
        .count()
        .withColumnRenamed("count", count_alias)
    )


def get_pivoted_count(
    df: DataFrame,
    pivot_col: str,
    pivoted_col_prefix: str,
    group_col: str = "site_id",
) -> DataFrame:
    """
    Creates pivoted dataframe with the given pivot column and pivoted column prefix
    :param df:
    :param pivot_col:
    :param pivoted_col_prefix:
    :param group_col:
    :return:
    """
    return (
        df.withColumn(
            "combined_col",
            sf.concat(sf.lit(pivoted_col_prefix), df[pivot_col]),
        )
        .groupBy(group_col)
        .pivot("combined_col")
        .count()
    )
