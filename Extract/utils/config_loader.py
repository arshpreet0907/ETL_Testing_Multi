"""
utils/config_loader.py
-----------------------
Purpose : Load a YAML configuration file and return its contents as a plain dict.
          Central place for config I/O so all pipeline files share the same
          error handling and never scatter open() calls around the codebase.

Usage   :
    from utils.config_loader import load_config
    cfg = load_config("config/source_config.yaml")
    host = cfg["host"]
"""

import os
from typing import Any, Dict

import yaml  # type: ignore


def load_config(path: str) -> Dict[str, Any]:
    """
    Read a YAML file and return its top-level contents as a dict.

    Parameters
    ----------
    path : str
        Absolute or relative path to the YAML file.

    Returns
    -------
    dict
        Parsed YAML contents.  An empty file returns an empty dict.

    Raises
    ------
    FileNotFoundError
        If the file does not exist at the given path.
    ValueError
        If the file exists but cannot be parsed as valid YAML, or if the
        top-level YAML value is not a mapping (dict).
    """
    abs_path = os.path.abspath(path)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(
            f"Config file not found: {abs_path}\n"
            f"Ensure the file exists before running the pipeline."
        )

    with open(abs_path, "r", encoding="utf-8") as fh:
        try:
            contents = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ValueError(
                f"Failed to parse YAML config at {abs_path}: {exc}"
            ) from exc

    if contents is None:
        return {}

    if not isinstance(contents, dict):
        raise ValueError(
            f"Expected a YAML mapping (dict) at the top level of {abs_path}, "
            f"got {type(contents).__name__}."
        )

    return contents
