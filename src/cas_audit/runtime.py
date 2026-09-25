"""Notebook runtime helpers.

Bridges the Fabric notebook environment (``notebookutils``) and plain local
execution so the same script runs under Data Factory and under ``python -m``.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def new_run_id() -> str:
    """Correlation id stamped on rows and DQ results for one pipeline run.

    Data Factory passes its own pipeline run id in; we only mint one when
    running standalone.
    """
    return uuid.uuid4().hex


def exit_notebook(payload: dict[str, Any]) -> None:
    """Return a result to the orchestrator.

    Fabric Data Factory reads this via ``@activity('<notebook>').output.result.exitValue``,
    which is how downstream activities branch on row counts or DQ status.
    """
    value = json.dumps(payload, default=str)
    try:
        import notebookutils  # type: ignore

        notebookutils.notebook.exit(value)
    except Exception:
        get_logger("cas_audit.runtime").info("exitValue: %s", value)


def bootstrap_path() -> None:
    """Make ``src`` importable when running as a Fabric notebook.

    In Fabric the repo is attached as a Git-synced workspace folder or the
    package is installed to the environment; locally we add ``src`` to sys.path.
    """
    from pathlib import Path

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "src"
        if candidate.is_dir() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            return
