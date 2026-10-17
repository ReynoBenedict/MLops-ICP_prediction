"""Prometheus metrics (scraped from /metrics; see infra/monitoring)."""

from __future__ import annotations

from prometheus_client import Counter, Histogram

HTTP_REQUESTS = Counter("icp_http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "icp_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
FORECAST_RUNS = Counter("icp_forecast_runs_total", "Forecast executions", ["status"])
PREDICTION_VALUE = Histogram(
    "icp_prediction_value_usd",
    "Distribution of predicted ICP prices (USD/bbl)",
    buckets=(20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 140, 160, 200),
)
JOBS = Counter("icp_jobs_total", "Background jobs by final status", ["job_type", "status"])
