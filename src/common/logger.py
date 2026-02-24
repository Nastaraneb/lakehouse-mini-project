import logging
from pathlib import Path
from contextvars import ContextVar
from datetime import datetime, timezone
from uuid import uuid4
import hashlib
import os

_RUN_ID: ContextVar[str] = ContextVar("run_id", default="-")
_RUN_STARTED_AT: ContextVar[str] = ContextVar("run_started_at", default="-")
_RUN_WINDOW: ContextVar[str] = ContextVar("run_window", default="-")
_PIPELINE_ID: ContextVar[str] = ContextVar("pipeline_id", default="-")


def _make_pipeline_id(started_at_iso: str) -> str:
    
    h = hashlib.sha256(started_at_iso.encode("utf-8")).hexdigest()
    return h[:10]


def start_run(run_window: str = "-", pipeline_name: str = "lakehouse-mini") -> dict:
    run_id = str(uuid4())[:8]
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    pipeline_id = _make_pipeline_id(f"{pipeline_name}|{started}")

    _RUN_ID.set(run_id)
    _RUN_STARTED_AT.set(started)
    _RUN_WINDOW.set(run_window)
    _PIPELINE_ID.set(pipeline_id)

    return {"run_id": run_id, "started_at": started, "window": run_window, "pipeline_id": pipeline_id}


class _RunContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _RUN_ID.get()
        record.pipeline_id = _PIPELINE_ID.get()
        record.run_started_at = _RUN_STARTED_AT.get()
        record.run_window = _RUN_WINDOW.get()
        return True


def get_logger(name: str):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level, logging.INFO))

    Path("logs").mkdir(exist_ok=True)
    log_file = Path("logs") / "pipeline.log"

    fmt = (
        "%(asctime)s | %(levelname)s | pipeline=%(pipeline_id)s | run=%(run_id)s | "
        "started=%(run_started_at)s | window=%(run_window)s | %(name)s | %(message)s"
    )
    formatter = logging.Formatter(fmt)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(_RunContextFilter())

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(_RunContextFilter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger