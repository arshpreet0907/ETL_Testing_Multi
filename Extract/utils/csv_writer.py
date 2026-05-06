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
    from Extract.utils.csv_writer import save_dataframe_as_csv
    save_dataframe_as_csv(df, "output/orders/source_enriched.csv")
"""

import glob
import json
import logging
import os
import shutil

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


ROWS_PER_PART = 100_000  # each Spark part file targets ~100K rows


def save_dataframe_as_csv(df: DataFrame, file_path: str) -> None:
    """
    Write a PySpark DataFrame to a single CSV file at `file_path`.

    Instead of coalesce(1) (which pulls all data into one partition and
    doubles memory), Spark writes multiple part files of ~100K rows each.
    The part files are then concatenated sequentially — one chunk at a time —
    so peak memory stays bounded to a single part file during the merge step.

    Also saves a companion .schema.json file alongside the CSV for type-safe reloading.
    """
    file_path = os.path.normpath(file_path)
    parent_dir = os.path.dirname(file_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    tmp_dir = file_path + "_tmp_spark"
    col_count = len(df.columns)

    logger.info("Writing DataFrame to temporary Spark directory: %s", tmp_dir)

    # Count rows to calculate number of partitions needed
    row_count = df.count()
    num_parts = max(1, (row_count + ROWS_PER_PART - 1) // ROWS_PER_PART)
    logger.info("Rows: %d — writing in %d part(s) of ~%d rows each", row_count, num_parts, ROWS_PER_PART)

    (
        df.repartition(num_parts)
        .write.mode("overwrite")
        .option("header", "true")
        .option("nullValue", "")
        .csv(tmp_dir)
    )

    # Collect and sort part files so output order is deterministic
    part_files = sorted(
        glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        or glob.glob(os.path.join(tmp_dir, "part-*"))
    )

    if not part_files:
        raise FileNotFoundError(
            f"Spark produced no part file in {tmp_dir}. "
            "Check Spark logs for write errors."
        )

    # Concatenate part files into one final CSV — sequentially, one chunk at a time
    # First part already has a header; skip the header line for all subsequent parts
    total_parts = len(part_files)
    with open(file_path, "wb") as out:
        for i, part in enumerate(part_files):
            with open(part, "rb") as src:
                if i > 0:
                    src.readline()  # skip repeated header
                shutil.copyfileobj(src, out)
            rows_done = min((i + 1) * ROWS_PER_PART, row_count)
            # print(f"  [{i + 1}/{total_parts}] merged part {i + 1} — ~{rows_done:,} / {row_count:,} rows written")

    logger.info("CSV saved: %s (%d rows, %d columns, %d parts merged)", file_path, row_count, col_count, len(part_files))

    schema_path = os.path.splitext(file_path)[0] + ".schema.json"
    with open(schema_path, "w", encoding="utf-8") as sf:
        sf.write(json.dumps(json.loads(df.schema.json()), indent=2))
    logger.info("Schema saved: %s", schema_path)

    shutil.rmtree(tmp_dir, ignore_errors=True)
