"""
utils/csv_writer.py
--------------------
Purpose : Save any PySpark DataFrame as a single, named CSV file.
          Accepts a DataFrame + target file path and handles the Spark
          coalesce(1) → part-file rename dance transparently.
Inputs  : df        — PySpark DataFrame to persist
          file_path — full destination path including filename, e.g.
                      "output/orders/source_enriched.csv"
Outputs : The CSV file written to `file_path` on the local filesystem.
Usage   :
    from utils.csv_writer import save_dataframe_as_csv
    save_dataframe_as_csv(df, "output/orders/source_enriched.csv")
"""

import glob
import json
import logging
import os
import shutil

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def save_dataframe_as_csv(df: DataFrame, file_path: str) -> None:
    """
    Write a PySpark DataFrame to a single CSV file at `file_path`.

    The function uses coalesce(1) to produce exactly one Spark part file,
    then renames it to the desired path so callers never have to deal with
    Spark's internal directory structure.

    If the DataFrame is already cached (e.g. from the comparator), the
    write proceeds directly without creating a second cached copy —
    avoiding a redundant Spark action.

    Also saves a companion .schema.json file alongside the CSV for type-safe reloading.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame to persist.  Must not be empty for the rename to succeed,
        though an empty DataFrame will produce a header-only CSV (valid).
    file_path : str
        Absolute or relative destination path, including the filename.
        Parent directories are created automatically.
        If the file already exists it is overwritten.

    Raises
    ------
    FileNotFoundError
        If Spark produced no part file in the temporary directory (should not
        happen under normal conditions).
    RuntimeError
        If more than one part file is found (coalesce(1) guarantee violated).
    """
    file_path = os.path.normpath(file_path)
    parent_dir = os.path.dirname(file_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    # Spark writes to a temp directory next to the final file
    tmp_dir = file_path + "_tmp_spark"

    logger.info("Writing DataFrame to temporary Spark directory: %s", tmp_dir)

    col_count = len(df.columns)
    row_count = 0

    # Check if already cached — if so, write directly without a second cache
    is_cached = df.is_cached
    if is_cached:
        # DF is already cached upstream — write straight through
        df.coalesce(1).write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
    else:
        # Not cached — cache the coalesced DF so the count and write share
        # a single materialisation
        df_cached = df.coalesce(1).cache()
        row_count = df_cached.count()
        df_cached.write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
        df_cached.unpersist()

    # Locate the single part file Spark produced
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))

    if not part_files:
        # Spark may have written without extension on some versions
        part_files = glob.glob(os.path.join(tmp_dir, "part-*"))

    if not part_files:
        raise FileNotFoundError(
            f"Spark produced no part file in {tmp_dir}. "
            "Check Spark logs for write errors."
        )

    if len(part_files) > 1:
        raise RuntimeError(
            f"Expected exactly 1 part file after coalesce(1), found {len(part_files)}: "
            f"{part_files}"
        )

    # Move the single part file to the desired destination
    shutil.move(part_files[0], file_path)

    # For pre-cached DFs, derive row count from the written file (header line excluded)
    if is_cached:
        # Count lines in the written CSV minus header — avoids a Spark action
        with open(file_path, "r", encoding="utf-8") as f:
            row_count = sum(1 for _ in f) - 1  # subtract header
            row_count = max(row_count, 0)

    logger.info("CSV saved: %s (%d rows, %d columns)", file_path, row_count, col_count)

    # Save DataFrame schema alongside CSV for type-safe reloading.
    # When CSVs are later loaded via load_csvs(), this JSON file is used
    # to apply the original schema so numeric/date types are preserved
    # and the comparator's normalisation works correctly.
    schema_path = os.path.splitext(file_path)[0] + ".schema.json"
    with open(schema_path, "w", encoding="utf-8") as sf:
        sf.write(json.dumps(json.loads(df.schema.json()), indent=2))
    logger.info("Schema saved: %s", schema_path)

    # Clean up the temporary Spark directory (_SUCCESS, .crc files, etc.)
    shutil.rmtree(tmp_dir, ignore_errors=True)
