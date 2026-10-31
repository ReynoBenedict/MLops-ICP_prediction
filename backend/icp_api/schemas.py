"""API contract (request/response models). Exported to packages/contracts/openapi.json and
TypeScript types for the frontend via ``npm run contracts``. Responses are validated against
these models by FastAPI before they leave the server."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PERIOD_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- errors / common --------------------------------------------------------------------------
class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorDetail


# --- health -----------------------------------------------------------------------------------
class LiveResponse(BaseModel):
    status: Literal["ok"] = "ok"


class DependencyStatus(BaseModel):
    name: str
    status: Literal["ok", "degraded", "down", "not_configured"]
    critical: bool
    detail: str | None = None
    latency_ms: float | None = None


class ReadinessResponse(BaseModel):
    """``environment``, ``version`` and ``checks`` are null unless the caller is a platform operator."""

    status: Literal["ready", "degraded", "not_ready"]
    environment: str | None = None
    version: str | None = None
    checks: list[DependencyStatus] | None = None


class SystemStatus(ReadinessResponse):
    environment: str
    version: str
    checks: list[DependencyStatus]
    request_id: str | None = None  # of this status call, for correlating server logs
    metadata_backend: str
    object_store_backend: str
    schema_version: int | None
    expected_schema_version: int
    model_alias: str
    model_source: Literal["mlflow", "bundle"] = "mlflow"
    admin_enabled: bool


class WhoAmI(BaseModel):
    authenticated: bool
    name: str | None
    role: Literal["public", "viewer", "analyst", "admin", "owner", "operator"]
    kind: Literal["public", "user", "operator"] = "public"
    org_id: str | None = None
    org_name: str | None = None
    plan: str | None = None
    must_change_password: bool = False
    horizon_max: int = 3


# --- data -------------------------------------------------------------------------------------
class QualityIssue(BaseModel):
    code: str
    message: str
    periods: list[str] = Field(default_factory=list)


class QualityReport(BaseModel):
    dataset: str
    ok: bool
    row_count: int
    period_start: str | None
    period_end: str | None
    errors: list[QualityIssue]
    warnings: list[QualityIssue]
    missing_periods: list[str]
    duplicate_periods: list[str]
    stats: dict[str, float | None]


class Freshness(BaseModel):
    status: Literal["fresh", "lagging", "stale", "unknown"]
    latest_period: str | None
    expected_period: str | None
    months_behind: int | None


class ChangedValue(BaseModel):
    period: str
    old: float
    new: float


class DatasetDiff(BaseModel):
    added: list[str]
    removed: list[str]
    changed: list[ChangedValue]
    unchanged: int


class DatasetVersion(BaseModel):
    id: str
    dataset: str
    version: int
    sha256: str
    size_bytes: int
    row_count: int
    period_start: str | None
    period_end: str | None
    source: str
    source_detail: str | None
    quality: QualityReport
    diff: DatasetDiff | None
    parent_version_id: str | None
    is_active: bool
    created_at: str
    created_by: str
    provenance: dict[str, Any] | None = None


class DatasetVersionList(BaseModel):
    items: list[DatasetVersion]
    next_cursor: str | None


class IngestionRun(BaseModel):
    id: str
    dataset: str
    connector: str
    status: Literal["succeeded", "failed", "no_change"]
    started_at: str
    finished_at: str
    job_id: str | None
    requested_by: str
    summary: dict[str, Any]
    version_id: str | None


class IngestionRunList(BaseModel):
    items: list[IngestionRun]
    next_cursor: str | None


class DatasetProvenance(BaseModel):
    dataset: str
    label_id: str
    label_en: str
    unit: str
    provider: str
    definition: str
    definition_id: str = ""
    publication_lag_months: int
    last_ingestion: IngestionRun | None = None
    active_version: DatasetVersion | None
    freshness: Freshness


class ActiveDatasets(BaseModel):
    datasets: dict[str, DatasetProvenance]


class SourceEvidence(BaseModel):
    id: str
    dataset: str
    period: str
    value: float | None
    kind: str
    verification_status: str
    source_url: str
    document_sha256: str | None
    reference: str | None
    retrieved_at: str
    note: str | None
    dataset_version_id: str | None
    created_at: str


class EvidenceResponse(BaseModel):
    dataset: str
    period: str | None
    items: list[SourceEvidence]


class HistoryPoint(BaseModel):
    period: str
    icp_price: float | None = None
    wti_price: float | None = None
    brent_price: float | None = None
    dubai_price: float | None = None
    usd_idr: float | None = None
    usd_idr_eom: float | None = None
    icp_idr: float | None = None


class HistoryResponse(BaseModel):
    points: list[HistoryPoint]
    history_years: int | None = Field(
        None,
        description="Plan history limit applied to this response in years (null = unlimited).",
    )
    earliest_period: str | None = Field(
        None,
        description="Earliest month the plan may see (YYYY-MM); earlier `start` values are clipped to it.",
    )


class DatasetSummary(BaseModel):
    available: bool
    label_id: str
    unit: str
    provider: str | None = None
    error_code: str | None = None
    version_id: str | None = None
    version: int | None = None
    observations: int | None = None
    period_start: str | None = None
    period_end: str | None = None
    latest_value: float | None = None
    previous_value: float | None = None
    mom_change_pct: float | None = None
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    missing_periods: list[str] = Field(default_factory=list)
    warning_count: int = 0
    freshness: Freshness | None = None
    source_detail: str | None = None
    updated_at: str | None = None


class Relationship(BaseModel):
    correlation: float
    spread_mean: float
    spread_latest: float
    spread_latest_period: str
    months: int


class IcpIdr(BaseModel):
    period: str
    icp_usd: float
    usd_idr: float
    icp_idr: float
    fx_definition: str
    note: str


class DataSummary(BaseModel):
    datasets: dict[str, DatasetSummary]
    relationships: dict[str, Relationship]
    icp_idr: IcpIdr | None = None


class DataQualityResponse(BaseModel):
    dataset: str
    version_id: str
    report: QualityReport


class ImportResponse(BaseModel):
    dataset: str
    mode: Literal["merge", "replace"]
    valid: bool
    committed: bool
    requires_confirmation: bool
    upload_report: QualityReport
    resulting_report: QualityReport | None = None
    diff: DatasetDiff | None
    version: DatasetVersion | None
    created: bool | None = None
    note: str | None = None


# --- forecasts --------------------------------------------------------------------------------
def _check_scenario_values(v: dict[str, float], name: str, lo: float, hi: float) -> dict[str, float]:
    import re

    if len(v) > 6:
        raise ValueError(f"at most 6 {name} scenario months")
    for key, value in v.items():
        if not re.match(PERIOD_PATTERN, key):
            raise ValueError(f"invalid period {key!r}; expected YYYY-MM")
        if not lo < value <= hi:
            raise ValueError(f"{name} value for {key} must be in ({lo:g}, {hi:g}]")
    return v


class ScenarioInput(Strict):
    """Assumed monthly averages for months inside the forecast window (YYYY-MM -> value).

    Benchmarks in USD/bbl; ``usd_idr`` in rupiah per USD (only affects the rupiah equivalent).
    Months without an assumption use observed data when the month has ended, otherwise the last
    known value - and every output month says which applied.
    """

    brent: dict[str, float] = Field(default_factory=dict)
    dubai: dict[str, float] = Field(default_factory=dict)
    wti: dict[str, float] = Field(default_factory=dict)
    usd_idr: dict[str, float] = Field(default_factory=dict)

    @field_validator("brent", "dubai", "wti")
    @classmethod
    def _bench(cls, v: dict[str, float], info: Any) -> dict[str, float]:
        return _check_scenario_values(v, info.field_name, 0, 500)

    @field_validator("usd_idr")
    @classmethod
    def _fx(cls, v: dict[str, float]) -> dict[str, float]:
        return _check_scenario_values(v, "usd_idr", 1000, 100000)


class ForecastRequest(Strict):
    horizon: int = Field(1, ge=1, le=6, description="Months after the last official ICP month (1-6).")
    scenario: ScenarioInput = Field(default_factory=ScenarioInput)
    wti_overrides: dict[str, float] = Field(
        default_factory=dict, description="Deprecated alias for scenario.wti (kept for older clients)."
    )

    @field_validator("wti_overrides")
    @classmethod
    def _check_overrides(cls, v: dict[str, float]) -> dict[str, float]:
        return _check_scenario_values(v, "wti", 0, 500)

    def scenario_inputs(self) -> dict[str, dict[str, float]]:
        out = self.scenario.model_dump()
        out["wti"] = {**self.wti_overrides, **out["wti"]}
        return {k: v for k, v in out.items() if v}


class BenchmarkInput(BaseModel):
    value: float | None
    source: Literal["observed", "scenario", "assumed_hold_last", "unavailable"]
    used_by_model: bool


ForecastKind = Literal["estimate_awaiting_publication", "future_forecast", "legacy_record"]


class Localized(BaseModel):
    id: str
    en: str


class EvaluationBaseline(BaseModel):
    method: Literal["naive_last"]
    label: Localized
    mae: float | None
    n: int | None


class CoveragePeriod(BaseModel):
    origin_start: str
    origin_end: str


class PointEvaluation(BaseModel):
    mode: Literal["observed", "hold_last"]
    horizon: int
    n: int
    mae: float | None
    mase: float | None
    dir_acc: float | None = Field(description="Share of backtest cases where the up/down direction was right")
    coverage: dict[str, float] | None = Field(
        description="Interval level ('0.5','0.8','0.95') -> share of backtest actuals inside the range"
    )
    coverage_n: int | None
    coverage_embargoed: dict[str, float] | None = Field(
        None,
        description="Out-of-sample coverage recomputed from the stored backtest predictions with a fit/test "
        "embargo (no fit case targets a month after the first test origin); null when not derivable",
    )
    coverage_embargoed_n: int | None = None
    coverage_check: Literal["embargoed", "without_embargo"] | None = Field(
        None,
        description="embargoed: the stored coverage equals the embargoed recomputation; without_embargo: the stored "
        "figure predates the embargo (models exported before 2026-10-20) and differs; null: not derivable",
    )
    origin_start: str | None = Field(
        None, description="First backtest forecast origin (evaluation period start)"
    )
    origin_end: str | None = Field(None, description="Last backtest forecast origin (evaluation period end)")
    baseline: EvaluationBaseline | None = Field(
        None, description="Naive persistence on exactly the same backtest cases (null if not stored)"
    )
    beats_baseline: bool | None = None
    coverage_period: CoveragePeriod | None = Field(
        None, description="Origins of the out-of-sample interval coverage check"
    )
    under_coverage: list[str] = Field(
        default_factory=list,
        description="Levels whose measured coverage (stricter of stored and embargoed) is >= 0.10 below nominal "
        "or whose miss rate exceeds 1.5x the nominal miss rate (95 % level: below 0.925)",
    )
    coverage_warning: Localized | None = None
    summary: Localized | None = Field(
        None, description="Plain sentence: period, n, horizon, mode, error vs baseline"
    )


class PointActual(BaseModel):
    period: str
    value: float
    basis: str
    label: Localized


class InputUsage(BaseModel):
    input: Literal["brent", "dubai", "wti", "usd_idr"]
    used_by_method: bool
    scenario_months: list[str]
    note: Localized
    effect: Literal["used", "ignored", "rupiah_only"] | None = Field(
        None,
        description="used: changes the ICP forecast; ignored: the active method does not read it; "
        "rupiah_only: only the rupiah equivalent changes",
    )


class ForecastNotes(BaseModel):
    method: Localized
    interval: Localized
    evaluation: Localized
    coverage_warning: Localized | None = Field(
        None,
        description="Set when a shown point's interval under-covered in backtests (or was checked on few cases)",
    )
    ignored_inputs: Localized | None = None


class PlanClip(BaseModel):
    horizon_max: int
    stored_horizon: int
    clipped: bool


class ForecastPoint(BaseModel):
    period: str
    step: int
    kind: ForecastKind = Field(
        description="estimate_awaiting_publication: month ended, ICP not yet published (benchmarks observed); "
        "future_forecast: month not ended, conditional on assumed/scenario benchmarks."
    )
    prediction: float = Field(description="ICP estimate, USD/bbl")
    intervals: dict[str, list[float]] = Field(
        description="level (e.g. '0.8') -> [lower, upper], empirical backtest quantiles"
    )
    interval_basis: Literal["observed", "hold_last"] | None
    conditional_on_scenario: bool
    benchmarks: dict[str, BenchmarkInput]
    recursive: bool
    prediction_idr: float | None
    fx_rate: float | None
    fx_source: Literal["observed", "scenario", "assumed_hold_last"] | None
    evaluation: PointEvaluation | None = Field(
        None, description="Backtest facts for this step and input situation (read time)"
    )
    actual: PointActual | None = Field(
        None, description="Official ICP for this month once published (read time)"
    )
    kind_now: (
        Literal["estimate_awaiting_publication", "future_forecast", "legacy_record", "official_published"]
        | None
    ) = None


class ForecastModelInfo(BaseModel):
    version: str
    method: str
    method_family: str
    rule_based: bool
    description: str


class ForecastResult(BaseModel):
    forecast_origin: str = Field(description="Last official ICP month the forecast is anchored on (YYYY-MM).")
    issue_date: str = Field(description="Date the forecast was produced (YYYY-MM-DD).")
    horizon: int
    method: str
    points: list[ForecastPoint]
    warnings: list[str]
    interval_levels: list[float]
    interval_note: str
    model: ForecastModelInfo | None = None
    notes: ForecastNotes | None = None
    input_usage: list[InputUsage] = Field(default_factory=list)
    ignored_inputs: list[str] = Field(
        default_factory=list,
        description="Scenario inputs the active method does not read (flagged, not rejected)",
    )


class ForecastRun(BaseModel):
    id: str
    created_at: str
    request_id: str | None
    requested_by: str
    status: Literal["succeeded", "failed"]
    horizon: int
    workflow: str | None = None
    issue_date: str | None = None
    forecast_origin: str | None = None
    method: str | None = None
    model_name: str | None
    model_version: str | None
    model_run_id: str | None
    icp_dataset_version_id: str | None
    wti_dataset_version_id: str | None
    dataset_versions: dict[str, str] | None = None
    last_observed_period: str | None
    scenario: dict[str, dict[str, float]] | None = None
    scenario_id: str | None = None
    org_id: str | None = None
    params: dict[str, Any]
    result: ForecastResult | None
    error_code: str | None
    error_message: str | None
    duration_ms: int | None
    export_available: bool
    plan_clip: PlanClip | None = Field(
        None, description="Customers: points beyond the plan horizon are removed at read time"
    )


class ForecastRunList(BaseModel):
    items: list[ForecastRun]
    next_cursor: str | None


# --- models -----------------------------------------------------------------------------------
class HorizonMetrics(BaseModel):
    mode: Literal["observed", "hold_last"]
    horizon: int
    n: int
    mae: float | None = None
    rmse: float | None = None
    bias: float | None = None
    smape: float | None = None
    mase: float | None = None
    dir_acc: float | None = None


class ModelMetrics(BaseModel):
    score: float | None = Field(
        None, description="Selection score (configured metric averaged over the selection horizons)."
    )
    mae: float | None = Field(None, description="1-month-ahead MAE with observed benchmarks (USD/bbl).")
    rmse: float | None
    smape: float | None = None
    baseline_naive_last_mae: float | None = None
    baseline_naive_last_rmse: float | None
    hold_last_h1_mae: float | None = Field(
        None, description="1-month MAE when benchmarks are held at the last known value (no scenario)."
    )
    baseline_naive_hold_last_h1_mae: float | None = None
    by_horizon: list[HorizonMetrics] = Field(default_factory=list)


class DatasetRef(BaseModel):
    version_id: str | None
    sha256: str | None


class ModelVersion(BaseModel):
    version: str
    run_id: str | None
    status: str
    aliases: list[str]
    created_at_ms: int
    candidate: str | None
    method: str | None
    method_family: str | None
    rule_based: bool | None
    description: str | None
    benchmarks_used: list[str]
    requires: list[str]
    legacy: bool = Field(
        description="True for versions registered before the 2026-10 forecasting redesign (not servable)."
    )
    metrics: ModelMetrics
    fit_period: str | None
    backtest_origins: str | None
    last_official_period: str | None
    selection: str | None
    training_job_id: str | None
    git_commit: str | None
    config_sha256: str | None
    datasets: dict[str, DatasetRef]


class HoldoutPoint(BaseModel):
    period: str
    actual: float
    predicted: float


class BacktestMetricRow(BaseModel):
    method: str
    mode: str
    horizon: int
    n: int
    mae: float | None = None
    rmse: float | None = None
    bias: float | None = None
    smape: float | None = None
    mase: float | None = None
    dir_acc: float | None = None
    q05: float | None = None
    q10: float | None = None
    q90: float | None = None
    q95: float | None = None


class DisclosureRow(BaseModel):
    mode: Literal["observed", "hold_last"]
    horizon: int
    n: int
    mae: float | None
    baseline_mae: float | None
    beats_baseline: bool | None
    coverage: dict[str, float] | None
    coverage_n: int | None
    coverage_embargoed: dict[str, float] | None = Field(
        None,
        description="Out-of-sample coverage recomputed from the stored backtest predictions with a fit/test "
        "embargo (no fit case targets a month after the first test origin); null when not derivable",
    )
    coverage_embargoed_n: int | None = None
    coverage_check: Literal["embargoed", "without_embargo"] | None = Field(
        None,
        description="embargoed: the stored coverage equals the embargoed recomputation; without_embargo: the stored "
        "figure predates the embargo (models exported before 2026-10-20) and differs; null: not derivable",
    )
    under_coverage: list[str]
    coverage_warning: Localized | None


class EvaluationDisclosure(BaseModel):
    origin_start: str | None
    origin_end: str | None
    baseline_method: Literal["naive_last"]
    baseline_label: Localized
    calibrated: Literal[False] = Field(
        description="Always false: ranges are empirical backtest quantiles; see measured coverage per row"
    )
    coverage_period: CoveragePeriod | None
    rows: list[DisclosureRow]
    note: Localized


class ModelVersionDetail(ModelVersion):
    holdout_predictions: list[HoldoutPoint] = Field(
        description="1-month-ahead backtest pairs with observed benchmarks."
    )
    params: dict[str, str]
    intervals: dict[str, dict[str, dict[str, list[float]]]] | None = None
    interval_coverage: dict[str, dict[str, dict[str, float]]] | None = None
    model_card: str | None = None
    backtest_metrics: list[BacktestMetricRow] = Field(default_factory=list)
    evaluation_disclosure: EvaluationDisclosure | None = Field(
        None,
        description="Backtest period, n, mode, horizon, naive baseline and measured coverage (Agent D v6)",
    )


class ModelList(BaseModel):
    model_name: str
    alias: str
    production_version: str | None
    versions: list[ModelVersion]


class ExperimentRun(BaseModel):
    run_id: str
    run_name: str | None
    status: str
    start_time_ms: int | None
    end_time_ms: int | None
    parent_run_id: str | None
    training_job_id: str | None
    candidate: str | None
    method_family: str | None = None
    rule_based: bool = False
    metrics: dict[str, float]
    params: dict[str, str]
    fit_period: str | None = None
    backtest_origins: str | None = None


class ExperimentRunList(BaseModel):
    items: list[ExperimentRun]
    next_cursor: str | None


class ValidationCheck(BaseModel):
    name: str
    passed: bool
    detail: str


class ValidationReport(BaseModel):
    version: str
    passed: bool
    checks: list[ValidationCheck]
    candidate_score: float | None
    production_version: str | None
    production_score: float | None


class PromoteRequest(Strict):
    reason: str = Field(..., min_length=5, max_length=500)
    force: bool = Field(
        False, description="Override failed metric gates (never overrides load/schema checks)."
    )


class RollbackRequest(Strict):
    reason: str = Field(..., min_length=5, max_length=500)


class PromotionResult(BaseModel):
    from_version: str | None
    to_version: str
    validation: ValidationReport | None = None
    forced: bool = False


class PromotionRecord(BaseModel):
    id: str
    created_at: str
    model_name: str
    action: Literal["promote", "rollback"]
    from_version: str | None
    to_version: str
    reason: str
    forced: bool
    validation: dict[str, Any] | None
    actor: str
    request_id: str | None


class PromotionList(BaseModel):
    items: list[PromotionRecord]
    next_cursor: str | None


class DriftFeature(BaseModel):
    psi: float
    status: Literal["stable", "moderate", "significant"]
    reference_mean: float
    current_mean: float


class DriftResponse(BaseModel):
    model_version: str
    method: str | None = None
    reference_period: str
    current_period: str
    reference_rows: int
    current_rows: int
    features: dict[str, DriftFeature]
    max_psi: float
    status: Literal["stable", "moderate", "significant"]
    note: str


# --- jobs -------------------------------------------------------------------------------------
JobType = Literal[
    "train", "ingest_benchmarks", "ingest_fx", "ingest_icp", "refresh_forecast", "evaluate_alerts", "selftest"
]


class TrainParams(Strict):
    auto_promote: bool = Field(
        False, description="Promote the new version only if every validation gate passes."
    )


class IngestBenchmarksParams(Strict):
    allow_revisions: bool = Field(
        True, description="Accept upstream revisions of already-stored benchmark months."
    )


class IngestFxParams(Strict):
    allow_revisions: bool = Field(
        False, description="JISDOR fixings are not revised; keep stored values unless set."
    )


class IngestIcpParams(Strict):
    news_pages: int = Field(
        6, ge=1, le=120, description="How many pages of the ESDM news index to scan for announcements."
    )
    full_backfill: bool = Field(False, description="Scan the whole news archive (slow; first run only).")


class RefreshForecastParams(Strict):
    horizon: int = Field(6, ge=1, le=6, description="Horizon of the published system forecast.")


class EvaluateAlertsParams(Strict):
    org_id: str | None = Field(None, description="Evaluate one organisation only (default: all).")


class SelftestParams(Strict):
    sleep_seconds: float = Field(0, ge=0, le=60)
    mode: Literal["ok", "fail", "transient"] = "ok"


class JobCreateRequest(Strict):
    job_type: JobType
    params: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(None, pattern=r"^[A-Za-z0-9._:-]{8,128}$")


class Job(BaseModel):
    id: str
    job_type: str
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    params: dict[str, Any]
    result: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    attempt: int
    max_attempts: int
    retry_of: str | None
    timeout_seconds: int
    cancel_requested: bool
    requested_by: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    heartbeat_at: str | None


class JobList(BaseModel):
    items: list[Job]
    next_cursor: str | None


# --- audit ------------------------------------------------------------------------------------
class AuditEvent(BaseModel):
    id: str
    created_at: str
    actor: str
    action: str
    target: str | None
    outcome: Literal["success", "failure", "denied"]
    request_id: str | None
    details: dict[str, Any] | None


class AuditList(BaseModel):
    items: list[AuditEvent]
    next_cursor: str | None


# --- accounts & organisations -------------------------------------------------------------------
# Deliberately simple: one "@", no whitespace, a dot in the domain (deliverability is not checked).
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


class RegisterRequest(Strict):
    organization_name: str = Field(..., min_length=2, max_length=120)
    email: str = Field(..., min_length=5, max_length=200, pattern=EMAIL_PATTERN)
    name: str = Field(..., min_length=1, max_length=120)
    password: str = Field(..., min_length=10, max_length=200)
    accept_terms: bool = Field(
        ..., description="Must be true: acceptance of Syarat Penggunaan and Kebijakan Privasi."
    )


class LoginRequest(Strict):
    email: str = Field(..., min_length=5, max_length=200)
    password: str = Field(..., min_length=1, max_length=200)


class ChangePasswordRequest(Strict):
    current_password: str = Field(..., min_length=1, max_length=200)
    new_password: str = Field(..., min_length=10, max_length=200)


class ProfileUpdateRequest(Strict):
    name: str = Field(..., min_length=1, max_length=120)


class PlanInfo(BaseModel):
    code: str
    label_id: str
    label_en: str
    price_note_id: str
    price_note_en: str = ""
    seats: int
    forecast_horizon_max: int
    scenarios: bool
    saved_scenarios_max: int
    reports_pdf: bool
    reports_excel: bool
    alerts: bool
    alert_rules_max: int
    api_access: bool
    api_requests_per_month: int
    history_years: int
    insights: bool


class UserPublic(BaseModel):
    id: str
    org_id: str
    email: str
    name: str
    role: Literal["owner", "admin", "analyst", "viewer"]
    status: Literal["active", "disabled"]
    must_change_password: bool
    created_at: str
    last_login_at: str | None
    temporary_login_expires_at: str | None = Field(
        None,
        description="Set while a temporary password (invitation/reset) is pending; sign-in fails after it.",
    )
    email_verified: bool = Field(False, description="True once the address was confirmed by an e-mail link.")


class OrganizationPublic(BaseModel):
    id: str
    name: str
    slug: str
    plan: str
    status: Literal["active", "suspended"]
    created_at: str
    entitlements: PlanInfo


class SessionInfo(BaseModel):
    id: str
    created_at: str
    expires_at: str
    last_seen_at: str | None
    revoked_at: str | None
    user_agent: str | None


class AuthResponse(BaseModel):
    token: str = Field(
        description="Opaque session token; send as 'Authorization: Bearer <token>'. Shown once."
    )
    expires_at: str
    user: UserPublic
    organization: OrganizationPublic


class MeResponse(BaseModel):
    user: UserPublic
    organization: OrganizationPublic
    sessions: list[SessionInfo]


class OrganizationDetail(OrganizationPublic):
    members: int
    active_members: int
    saved_scenarios: int
    alert_rules: int
    usage_this_month: dict[str, int]
    plans: list[PlanInfo]


class OrgRenameRequest(Strict):
    name: str = Field(..., min_length=2, max_length=120)


class InviteRequest(Strict):
    email: str = Field(..., min_length=5, max_length=200, pattern=EMAIL_PATTERN)
    name: str = Field(..., min_length=1, max_length=120)
    role: Literal["admin", "analyst", "viewer"] = "analyst"


class InviteResponse(BaseModel):
    member: UserPublic
    temporary_password: str
    note: str


class MemberUpdateRequest(Strict):
    role: Literal["admin", "analyst", "viewer"] | None = None
    status: Literal["active", "disabled"] | None = None


class TemporaryPasswordResponse(BaseModel):
    temporary_password: str
    expires_at: str | None = Field(None, description="The temporary password stops working after this time.")


# --- identity lifecycle (v7 Wave B) ---------------------------------------------------------------
class ActiveSession(BaseModel):
    """One active session of the signed-in user. No token material, no raw user agent."""

    id: str
    device: str = Field(..., description="Short device label derived from the user agent")
    created_at: str
    last_seen_at: str | None
    expires_at: str = Field(..., description="Absolute expiry (30 days after sign-in)")
    idle_expires_at: str | None = Field(
        None, description="The session ends if unused until this time (SESSION_IDLE_TIMEOUT_HOURS)"
    )
    current: bool = Field(..., description="True for the session making this request")


class RevokedSessions(BaseModel):
    revoked: int


class PasswordResetRequest(Strict):
    email: str = Field(..., min_length=5, max_length=200, pattern=EMAIL_PATTERN)


class PasswordResetConfirm(Strict):
    token: str = Field(..., min_length=10, max_length=200)
    new_password: str = Field(..., min_length=10, max_length=200)


class EmailTokenConfirm(Strict):
    token: str = Field(..., min_length=10, max_length=200)


class EmailRequestAccepted(BaseModel):
    status: Literal["accepted"] = "accepted"
    note: str


class ApiKeyCreateRequest(Strict):
    name: str = Field(..., min_length=1, max_length=80)


class ApiKeyCreated(BaseModel):
    id: str
    name: str
    prefix: str
    key: str
    note: str


class ApiKeyInfo(BaseModel):
    id: str
    org_id: str
    name: str
    prefix: str
    created_by: str
    created_at: str
    last_used_at: str | None
    revoked_at: str | None


class OperatorPlanRequest(Strict):
    plan: Literal["trial", "starter", "professional", "enterprise"]
    reason: str = Field(..., min_length=5, max_length=500)


PlanCode = Literal["trial", "starter", "professional", "enterprise"]


class PlanChangeRequestCreate(Strict):
    requested_plan: PlanCode
    note: str | None = Field(None, max_length=500)


class PlanChangeResolveRequest(Strict):
    decision: Literal["approve", "decline"]
    note: str | None = Field(None, max_length=500)


class PlanChangeRequest(BaseModel):
    id: str
    org_id: str
    org_name: str | None
    requested_by: str
    requested_by_name: str | None
    previous_plan: str
    requested_plan: PlanCode
    note: str | None
    status: Literal["pending", "approved", "declined", "withdrawn"]
    created_at: str
    resolved_at: str | None
    resolved_by: str | None
    resolution_note: str | None


class PlanChangeRequestList(BaseModel):
    items: list[PlanChangeRequest]
    next_cursor: str | None


class DeletionRequestCreate(Strict):
    """``account``: delete the caller's own account; ``organization``: the owner deletes the organisation."""

    scope: Literal["account", "organization"]
    reason: str | None = Field(None, max_length=500)


class DeletionResolveRequest(Strict):
    decision: Literal["execute", "decline"]
    note: str | None = Field(None, max_length=500)


class DeletionRequest(BaseModel):
    id: str
    org_id: str
    org_name: str | None
    scope: Literal["account", "organization"]
    user_id: str | None
    user_name: str | None
    requested_by: str
    requested_by_name: str | None
    reason: str | None
    status: Literal["pending", "executed", "declined", "withdrawn"]
    created_at: str
    resolved_at: str | None
    resolved_by: str | None
    resolution_note: str | None


class DeletionRequestList(BaseModel):
    items: list[DeletionRequest]
    next_cursor: str | None


class OperatorStatusRequest(Strict):
    status: Literal["active", "suspended"]
    reason: str = Field(..., min_length=5, max_length=500)


class OrganizationList(BaseModel):
    items: list[dict[str, Any]]
    next_cursor: str | None


# --- product: snapshot, insights, scenarios, alerts, reports -----------------------------------
class SnapshotMonth(BaseModel):
    period: str = Field(description="YYYY-MM")
    icp_usd: float | None = Field(
        description="Official ICP (USD/bbl) from the active dataset; null when no official value exists "
        "for the month or its official documents disagree (verification basis conflict); never 0, never an "
        "estimate"
    )
    brent_usd: float | None = Field(description="Brent monthly average (USD/bbl) as stored; null if absent")


class SnapshotCoverage(BaseModel):
    first_period: str
    last_period: str
    months: int = Field(description="Months with an official ICP value")
    confirmed_months: int = Field(
        description="Months whose verification basis is two_documents or decree_text (same computation "
        "as GET /data/verification)"
    )


class SnapshotEvidenceDocument(BaseModel):
    kind: Literal["decree", "announcement"]
    reference: str | None = Field(
        description="The decree's own number, only when it is verifiable; always null for announcements"
    )
    published_on: str | None = Field(description="YYYY-MM-DD, only when stored")
    url: str | None = Field(description="Public government page stored as the source, else null")


class SnapshotEvidence(BaseModel):
    period: str
    basis: VerificationBasis
    label: Localized
    documents: list[SnapshotEvidenceDocument]


class MarketSnapshot(BaseModel):
    """Public, anonymous market snapshot for the landing page (no account required).

    ``latest_forecast`` is a teaser only - ``{available, issue_date, forecast_origin, horizon}`` -
    and never carries predicted values, intervals or method details (those require sign-in).
    ``recent``, ``coverage``, ``icp_evidence`` and ``generated_at`` (contract C1) are optional for
    older clients and carry official observations and benchmarks only, never forecast values.
    A month whose official documents disagree (verification basis ``conflict``) is withheld everywhere:
    its value is null in ``recent`` and, when it is the latest month, in ``icp_usd``, ``mom_change_pct``
    and ``icp_idr`` (``icp_evidence`` then names the basis); as the previous month it nulls
    ``previous_period``, ``previous_usd`` and ``mom_change_pct``.
    """

    icp_period: str | None
    icp_usd: float | None
    previous_period: str | None
    previous_usd: float | None
    mom_change_pct: float | None
    brent_usd: float | None
    brent_period: str | None
    usd_idr: float | None
    icp_idr: float | None
    freshness: dict[str, Any]
    source: str
    updated_at: str | None
    latest_forecast: dict[str, Any] | None = None
    recent: list[SnapshotMonth] = Field(
        default_factory=list,
        description="Last PUBLIC_HISTORY_MONTHS months ending at the latest official ICP month, ascending "
        "(empty when disabled or without ICP data)",
    )
    coverage: SnapshotCoverage | None = None
    icp_evidence: SnapshotEvidence | None = Field(
        None, description="Stored official documents for the latest ICP month; null when none exist"
    )
    generated_at: str | None = Field(None, description="ISO-8601 UTC time the response was generated")


class InsightMetric(BaseModel):
    label: str
    value: float | int | str | None
    unit: str


class InsightSource(BaseModel):
    dataset: str
    label: str
    provider: str
    version_id: str | None
    period_end: str | None


class InsightFact(BaseModel):
    key: str
    label: str
    value: float | int | str | None
    unit: str
    display: str = Field(description="Value formatted for the response language; render this.")


class InsightPeriods(BaseModel):
    subject: str | None
    window_start: str | None
    window_end: str | None
    n: int | None


class InsightMethod(BaseModel):
    key: str
    text: str


class InsightModelRef(BaseModel):
    version: str
    method: str


class InsightEvidence(BaseModel):
    datasets: list[InsightSource]
    periods: InsightPeriods
    inputs: list[InsightFact]
    outputs: list[InsightFact]
    method: InsightMethod
    model: InsightModelRef | None
    forecast_ids: list[str]


class InsightItem(BaseModel):
    id: str
    rule_id: str
    rule_version: int
    kind: str
    importance: Literal["high", "medium", "low"]
    severity: Literal["info", "notice", "warning"] = Field(
        description="compat: high=warning, medium=notice, low=info"
    )
    audience: Literal["customer", "operator"]
    title: str
    body: str
    period: str | None
    next_step: str | None
    facts: list[InsightFact]
    evidence: InsightEvidence
    metrics: list[InsightMetric] = Field(description="compat: same numbers as facts")
    sources: list[InsightSource] = Field(description="compat: same as evidence.datasets")
    generated: bool


class InsightsResponse(BaseModel):
    generated_at: str
    lang: Literal["id", "en"]
    engine_version: str
    basis: dict[str, Any]
    insights: list[InsightItem]
    disclaimer: str


VerificationBasis = Literal[
    "two_documents",
    "decree_text",
    "document_plus_legacy",
    "single_document",
    "ocr_single",
    "conflict",
    "no_evidence",
    "provider_published",
]


class DissentReading(BaseModel):
    value: float
    kind: str
    kind_label: Localized
    reference: str | None
    reason: Localized
    source_url: str | None = Field(description="Operators only; null for customers")


class VerificationMonth(BaseModel):
    period: str
    value: float | None
    status: Literal[
        "verified", "single_source", "machine_extracted", "conflict", "no_evidence", "provider_published"
    ]
    basis: VerificationBasis
    documents: int = Field(description="Distinct official ESDM documents agreeing with the value")
    kinds: list[str]
    reference: str | None
    label: Localized
    explanation: Localized
    dissent: list[DissentReading] = Field(
        default_factory=list,
        description="Source readings that differ from the value in use (outvoted or superseded), never silently dropped",
    )
    dissent_label: Localized | None = Field(
        None, description="'Sumber berbeda' summary; null when no dissent"
    )


class VerificationLegendItem(BaseModel):
    label: Localized
    explanation: Localized


class VerificationResponse(BaseModel):
    dataset: str
    history_years: int | None
    earliest_period: str | None
    counts: dict[str, int]
    dissent_months: int = Field(0, description="Months in range with at least one differing source reading")
    months: list[VerificationMonth]
    legend: dict[str, VerificationLegendItem]


class ScenarioCreateRequest(Strict):
    name: str = Field(..., min_length=1, max_length=120)
    description: str | None = Field(None, max_length=1000)
    horizon: int = Field(3, ge=1, le=6)
    inputs: ScenarioInput = Field(default_factory=ScenarioInput)


class ScenarioUpdateRequest(Strict):
    name: str | None = Field(None, min_length=1, max_length=120)
    description: str | None = Field(None, max_length=1000)
    horizon: int | None = Field(None, ge=1, le=6)
    inputs: ScenarioInput | None = None


class Scenario(BaseModel):
    id: str
    org_id: str
    created_by: str
    name: str
    description: str | None
    horizon: int
    inputs: dict[str, dict[str, float]]
    created_at: str
    updated_at: str
    last_forecast_id: str | None
    archived: bool
    input_usage: list[InputUsage] = Field(default_factory=list)


class ScenarioList(BaseModel):
    items: list[Scenario]
    next_cursor: str | None


class ScenarioCompareRequest(Strict):
    scenario_ids: list[str] = Field(..., min_length=1, max_length=4)
    horizon: int | None = Field(None, ge=1, le=6)


class ScenarioCompareResponse(BaseModel):
    horizon: int
    forecast_origin: str
    issue_date: str
    method: str
    baseline_forecast_id: str
    scenarios: list[dict[str, Any]]
    rows: list[dict[str, Any]]
    note: str


class AlertKind(BaseModel):
    kind: str
    label_id: str
    label_en: str
    params: dict[str, Any]


class AlertRuleCreateRequest(Strict):
    kind: Literal[
        "icp_published",
        "icp_change_pct",
        "benchmark_change_pct",
        "forecast_change",
        "data_stale",
        "ingestion_failed",
    ]
    params: dict[str, Any] = Field(default_factory=dict)


class AlertRuleUpdateRequest(Strict):
    params: dict[str, Any] | None = None
    enabled: bool | None = None


class AlertRule(BaseModel):
    id: str
    org_id: str
    created_by: str
    kind: str
    label_id: str | None = None
    label_en: str | None = None
    params: dict[str, Any]
    enabled: bool
    created_at: str
    updated_at: str
    last_evaluated_at: str | None
    last_triggered_at: str | None


class AlertEvent(BaseModel):
    id: str
    rule_id: str
    org_id: str
    created_at: str
    dedupe_key: str
    severity: Literal["info", "warning", "critical"]
    title: str
    body: str
    data: dict[str, Any] | None
    read_at: str | None
    title_en: str | None = None
    body_en: str | None = None
    period: str | None = Field(
        None, description="Month the event is about (e.g. the newly detected ICP month)"
    )
    link: dict[str, Any] | None = Field(
        None, description="Deep-link target: {kind: period|forecast|dataset, period?, forecast_id?, dataset?}"
    )


class AlertEventList(BaseModel):
    items: list[AlertEvent]
    next_cursor: str | None
    unread: int


DEFAULT_REPORT_FORMATS: list[Literal["pdf", "xlsx", "csv"]] = ["pdf", "xlsx", "csv"]


class ReportCreateRequest(Strict):
    kind: Literal["market_monthly", "forecast", "scenario_comparison", "data_provenance"]
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="forecast: {forecast_id | horizon, scenario}; scenario_comparison: {scenario_ids, horizon}",
    )
    formats: list[Literal["pdf", "xlsx", "csv"]] = Field(
        default_factory=lambda: list(DEFAULT_REPORT_FORMATS),
        min_length=1,
        description="At least one format. PDF needs a plan with PDF reports (403 otherwise); XLSX is dropped "
        "on plans without Excel reports, and an XLSX-only request then produces CSV (available on every plan).",
    )


class ReportRun(BaseModel):
    id: str
    org_id: str | None
    created_by: str
    kind: str
    label: str
    params: dict[str, Any]
    status: Literal["succeeded", "failed"]
    created_at: str
    finished_at: str | None
    formats: list[str]
    error_message: str | None
    summary: dict[str, Any] | None


class ReportList(BaseModel):
    items: list[ReportRun]
    next_cursor: str | None
