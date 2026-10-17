"""Job worker process entry point: ``python -m icp_api.worker <job_id> <result_json_path>``.

Writes ``{"status": "succeeded", "result": ...}`` or a sanitized failure to the result file.
Exit codes: 0 success, 75 transient failure (retryable), 1 permanent failure.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

from icp_api.container import build_container
from icp_api.errors import ApiError
from icp_api.logging_setup import configure_logging, request_id_var
from icp_api.services.jobs import EXIT_TRANSIENT, job_definitions
from icp_api.settings import load_settings
from icp_api.tasks import TASKS, JobInputError, TransientJobError

logger = logging.getLogger("icp_api.worker")


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")


def main(argv: list[str]) -> int:
    job_id, result_path = argv[0], Path(argv[1])
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_format)
    container = build_container(settings)
    job = container.repo.get_job(job_id)
    if job is None:
        _write(result_path, {"status": "failed", "error_code": "NOT_FOUND", "error_message": "job not found"})
        return 1
    request_id_var.set(job.get("request_id"))
    task = TASKS.get(job["job_type"])
    if task is None or job["job_type"] not in job_definitions(settings):
        _write(
            result_path,
            {"status": "failed", "error_code": "UNKNOWN_JOB_TYPE", "error_message": job["job_type"]},
        )
        return 1
    logger.info("job started", extra={"job_id": job_id, "job_type": job["job_type"]})
    try:
        result = task(container, job)
    except TransientJobError as exc:
        logger.warning("transient job failure: %s", exc, extra={"job_id": job_id})
        _write(
            result_path, {"status": "failed", "error_code": "TRANSIENT_FAILURE", "error_message": str(exc)}
        )
        return EXIT_TRANSIENT
    except JobInputError as exc:
        _write(result_path, {"status": "failed", "error_code": exc.code, "error_message": str(exc)})
        return 1
    except ApiError as exc:
        _write(result_path, {"status": "failed", "error_code": exc.code, "error_message": exc.message})
        return 1
    except Exception as exc:
        logger.exception("job failed", extra={"job_id": job_id})
        _write(
            result_path,
            {
                "status": "failed",
                "error_code": "JOB_FAILED",
                "error_message": f"{type(exc).__name__} (see backend logs for request/job id {job_id})",
            },
        )
        return 1
    _write(result_path, {"status": "succeeded", "result": result})
    logger.info("job succeeded", extra={"job_id": job_id})
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
