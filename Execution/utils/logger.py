"""
utils/logger.py
---------------
Provide a single, consistently configured logger factory.
Databricks version — no pipeline_config.yaml dependency.
"""

import logging
from typing import Optional

_DEFAULT_LEVEL = "INFO"
_resolved_level: Optional[int] = None
_LOG_FORMAT = "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_handler_configured = False


def _resolve_log_level() -> int:
    global _resolved_level
    if _resolved_level is not None:
        return _resolved_level
    _resolved_level = logging.INFO
    return _resolved_level


def _configure_root_handler() -> None:
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
    """Return a named logger configured consistently with the rest of the pipeline."""
    _configure_root_handler()
    return logging.getLogger(name)

