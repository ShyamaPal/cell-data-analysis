from pyspark.sql import DataFrame

from cell_data_analysis.df_utils import get_count_per_group, get_pivoted_count


def test_get_count_per_group(df: DataFrame):
    assert (
        get_count_per_group(
            cell_df=df, count_alias="CountCol", group_col="Col1"
        ).count()
        == 2
    )


def test_get_pivoted_count(df: DataFrame):
    assert (
        get_pivoted_count(
            df=df,
            pivot_col="Col2",
            pivoted_col_prefix="pivoted_col",
            group_col="Col1",
        ).count()
        == 2
    )
