"""Structured stdout logging, captured by journald under the systemd timer.

Discipline, not a filter: callers must never log secrets (the AppsFlyer API
token, the DB password) — only IDs, dates, and row counts. This module can't
enforce that; it just makes sure logs land somewhere useful.
"""

from __future__ import annotations

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """Idempotent: uses logging.basicConfig without force=True, so calling
    this more than once (e.g. once per CLI invocation) is a no-op after the
    first call, and it never tears down pytest's caplog handler.
    """
    logging.basicConfig(
        stream=sys.stdout,
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
