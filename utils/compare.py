"""
utils/compare.py
-----------------
Modular comparison module — compares two DataFrames and generates a report.
Databricks version — removed dead rulebook_loader import.
"""

import os
import sys
import time

from pyspark.sql import SparkSession, DataFrame

from utils.logger import get_logger
from utils.comparator import compare_dataframes
from utils.reporter import generate_report

logger = get_logger(__name__)


def compare_and_report(
    spark: SparkSession,
    source_df: DataFrame,
    target_df: DataFrame,
    primary_key_cols: list,
    compare_cols: list,
    output_path: str,
) -> int:
    """
    Compare two DataFrames and generate a diff report.

    Returns
    -------
    int
        Exit code: 0 (pass), 3 (differences found)
    """
    logger.info("Starting comparison on PK=%s", primary_key_cols)

    start = time.time()
    diff_df, src_count, tgt_count, matched_count, total_diffs = compare_dataframes(
        source_df=source_df,
        target_df=target_df,
        primary_key_cols=primary_key_cols,
        compare_cols=compare_cols,
    )
    logger.info("Comparison completed in %.2fs", time.time() - start)

    start = time.time()
    exit_code = generate_report(
        diff_df=diff_df,
        source_row_count=src_count,
        target_row_count=tgt_count,
        matched_row_count=matched_count,
        total_diff_count=total_diffs,
        output_path=output_path,
        exit_on_differences=False,
    )
    logger.info("Report generation completed in %.2fs", time.time() - start)

    return exit_code

