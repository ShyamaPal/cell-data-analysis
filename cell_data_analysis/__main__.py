import logging
import argparse

from cell_data_analysis.analysis import (
    get_cells_per_site,
    get_frequency_per_site,
)
from cell_data_analysis.connection import get_spark_connection
from cell_data_analysis.df_utils import read_csv_data_frame, write_data_frame

log = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pyspark util for cell data analysis"
    )
    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Pass the input file parent path",
    )
    parser.add_argument(
        "--output-path", type=str, required=True, help="Mention the output path"
    )
    args = parser.parse_args()
    log.info(
        "============================== Process started ==============================="
    )

    log.info("Creating spark session")
    spark = get_spark_connection(app_name="CellAnalysis")

    log.info("Loading gsm data-frame")
    gsm_df = read_csv_data_frame(
        spark=spark, parent_path=f"{args.input_path}/gsm"
    )

    log.info("Loading umts data-frame")
    umts_df = read_csv_data_frame(
        spark=spark, parent_path=f"{args.input_path}/umts"
    )

    log.info("Loading lte data-frame")
    lte_df = read_csv_data_frame(
        spark=spark, parent_path=f"{args.input_path}/lte"
    )

    log.info("Loading site data-frame")
    site_df = read_csv_data_frame(
        spark=spark, parent_path=f"{args.input_path}/site"
    )

    log.info("Calculate cells per site data-frame")
    cells_per_site_df = site_df.join(
        get_cells_per_site(gsm_df=gsm_df, umts_df=umts_df, lte_df=lte_df),
        "site_id",
    )
    log.info("Writing cells per site data-frame")
    write_data_frame(
        df=cells_per_site_df, output_path=f"{args.output_path}/cells_per_site"
    )

    log.info("Calculate frequency band per site data-frame")
    freq_per_site_df = site_df.join(
        get_frequency_per_site(gsm_df=gsm_df, umts_df=umts_df, lte_df=lte_df),
        "site_id",
    )
    log.info("Writing frequency per site data-frame")
    write_data_frame(
        df=freq_per_site_df,
        output_path=f"{args.output_path}/frequency_per_site",
    )
    log.info("Stopping spark session")
    spark.stop()
    log.info(
        "============================== Process ended ==============================="
    )
