"""
perform_transform.py
--------------------
Apply transformations to a PySpark DataFrame using a transform file.
Optionally saves the result to CSV.

Usage:
    from perform_transform import perform_transform
    
    df_transformed = perform_transform(
        df=source_df,
        transform_file="generated/orders/transform.py",
        rulebook=rulebook,
        save_csv="output/orders/source_enriched.csv"
    )
"""

import importlib.util
import logging
import os
import types
from typing import Optional,Literal

from pyspark.sql import DataFrame

from utils.csv_writer import save_dataframe_as_csv
from utils.logger import get_logger

logger = get_logger(__name__)


def perform_transform(
    df: DataFrame,
    transform_file: Optional[str] = None,
    rulebook: Optional[dict] = None,
    target_mode: Literal['mysql', 'snowflake'] = 'mysql',
) -> DataFrame:
    """
    Apply transformations to a DataFrame using a transform file.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame
    transform_file : str, optional
        Path to Python file containing apply_transforms(df, rulebook) function.
        If None, returns df unchanged.
    rulebook : dict, optional
        Rulebook dict to pass to transform function

    Returns
    -------
    DataFrame
        Transformed DataFrame

    Raises
    ------
    FileNotFoundError
        If transform_file does not exist
    AttributeError
        If transform_file has no apply_transforms function
    """
    if transform_file is None:
        logger.info("No transform file provided, returning DataFrame unchanged")
        return df

    logger.info("Applying transformations from: %s", transform_file)

    # Load transform function
    transform_func = _load_transform_function(transform_file)

    # Apply transformations
    try:
        if rulebook is not None:
            df_transformed = transform_func(df, rulebook)
        else:
            # Try calling with just df
            try:
                df_transformed = transform_func(df,target_mode)
            except TypeError:
                # Function requires rulebook, pass empty dict
                df_transformed = transform_func(df, {})
    except Exception as exc:
        logger.error("Transform function failed: %s", exc)
        raise

    logger.info(
        "Transformation complete: %d columns: %s",
        len(df_transformed.columns),
        df_transformed.columns
    )

    return df_transformed


def _load_transform_function(transform_file: str):
    """
    Dynamically import apply_transforms from the given transform.py path.

    Returns the callable apply_transforms(df, rulebook) -> DataFrame.
    """
    abs_path = os.path.abspath(transform_file)
    if not os.path.isfile(abs_path):
        raise FileNotFoundError(
            f"Transform file not found: {abs_path}\n"
            "Ensure the transform file exists before running."
        )

    spec = importlib.util.spec_from_file_location("_dynamic_transform", abs_path)
    module: types.ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "apply_transforms"):
        raise AttributeError(
            f"Transform file {abs_path} has no 'apply_transforms' function.\n"
            "Expected signature: apply_transforms(df, rulebook=None) -> DataFrame"
        )

    return module.apply_transforms
