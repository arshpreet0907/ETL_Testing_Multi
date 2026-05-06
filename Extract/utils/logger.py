"""
utils/logger.py
---------------
Purpose : Provide a single, consistently configured logger factory for all
          pipeline modules.  Log level is read once from pipeline_config.yaml
          so every module in the run shares the same level without each file
          having to manage its own handler setup.

Usage   :
    from Extract.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Starting extraction for table: %s", table_name)
"""

import logging
import os
from typing import Optional

# Default level used when pipeline_config.yaml is absent or unparseable
_DEFAULT_LEVEL = "INFO"

# Cache the resolved level so we only read the YAML once per process
_resolved_level: Optional[int] = None

_LOG_FORMAT = "[%(asctime)s] %(levelname)-8s %(name)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Whether the root handler has already been configured this process
_handler_configured = False


def _resolve_log_level() -> int:
    """
    Read log_level from config/pipeline_config.yaml.
    Falls back to INFO if the file is missing, unreadable, or the key is absent.
    """
    global _resolved_level
    if _resolved_level is not None:
        return _resolved_level

    level_name = _DEFAULT_LEVEL
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "pipeline_config.yaml",
    )

    try:
        import yaml  # type: ignore
        with open(config_path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        level_name = str(cfg.get("log_level", _DEFAULT_LEVEL)).upper()
    except FileNotFoundError:
        pass  # Use default — config file not yet created
    except Exception:
        pass  # Silently fall back; logger itself can't log here

    _resolved_level = getattr(logging, level_name, logging.INFO)
    return _resolved_level


def _configure_root_handler() -> None:
    """Attach a StreamHandler to the root logger exactly once per process."""
    global _handler_configured
    if _handler_configured:
        return

    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        root.addHandler(handler)

    root.setLevel(_resolve_log_level())
    _handler_configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger configured consistently with the rest of the pipeline.

    Parameters
    ----------
    name : str
        Typically pass ``__name__`` so log records carry the module path.

    Returns
    -------
    logging.Logger
        A logger whose level and handler are inherited from the root logger
        configured by this module.
    """
    _configure_root_handler()
    logger = logging.getLogger(name)
    # Do not set level on child loggers — let them inherit from root
    return logger
