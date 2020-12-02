from pyspark.sql import DataFrame

from cell_data_analysis.df_utils import get_count_per_group, get_pivoted_count


def get_cells_per_site(
    gsm_df: DataFrame, umts_df: DataFrame, lte_df: DataFrame
) -> DataFrame:
    """
    Retrieve the cells per technology in a site
    :param gsm_df:
    :param umts_df:
    :param lte_df:
    :return:
    """
    gsm_count_df = get_count_per_group(
        cell_df=gsm_df, count_alias="site_2g_cnt"
    )
    umts_count_df = get_count_per_group(
        cell_df=umts_df, count_alias="site_3g_cnt"
    )
    lte_count_df = get_count_per_group(
        cell_df=lte_df, count_alias="site_4g_cnt"
    )

    return (
        gsm_count_df.join(umts_count_df, "site_id", "full")
        .join(lte_count_df, "site_id", "full")
        .na.fill(0)
    )


def get_frequency_per_site(
    gsm_df: DataFrame, umts_df: DataFrame, lte_df: DataFrame
) -> DataFrame:
    """
    Returns dataframe with calculated frequency per site
    :param gsm_df:
    :param umts_df:
    :param lte_df:
    :return:
    """
    gsm_freq_cnt_df = get_pivoted_count(
        df=gsm_df,
        pivot_col="frequency_band",
        pivoted_col_prefix="frequency_band_G",
    )
    umts_freq_cnt_df = get_pivoted_count(
        df=umts_df,
        pivot_col="frequency_band",
        pivoted_col_prefix="frequency_band_U",
    )
    lte_freq_cnt_df = get_pivoted_count(
        df=lte_df,
        pivot_col="frequency_band",
        pivoted_col_prefix="frequency_band_L",
    )

    return (
        gsm_freq_cnt_df.join(umts_freq_cnt_df, "site_id", "full")
        .join(lte_freq_cnt_df, "site_id", "full")
        .na.fill(0)
    )
