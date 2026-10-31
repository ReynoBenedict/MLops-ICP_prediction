"""A failed report run is never listed or downloadable as a success (2026-10-25)."""

from __future__ import annotations

from typing import Any

from icp_api.storage.repository import new_id, utcnow

API = "/api/v1"


def _failed_rows(client: Any, h: dict[str, str]) -> list[dict[str, Any]]:
    return [
        i
        for i in client.get(f"{API}/reports?limit=100", headers=h).json()["items"]
        if i["status"] == "failed"
    ]


def test_failure_after_a_file_was_written_offers_no_file(client, customer, monkeypatch):
    from icp_api.services import reports as reports_mod

    c = client.app.state.container
    stored: list[str] = []
    real_put = c.objects.put

    def tracking_put(key: str, data: bytes, *a: Any, **kw: Any) -> Any:
        stored.append(key)
        return real_put(key, data, *a, **kw)

    def boom(self: Any, content: dict[str, Any]) -> bytes:
        raise RuntimeError("csv writer exploded")

    monkeypatch.setattr(c.objects, "put", tracking_put)
    monkeypatch.setattr(reports_mod.ReportService, "_csv", boom)
    before = {i["id"] for i in _failed_rows(client, customer)}
    r = client.post(
        f"{API}/reports", json={"kind": "market_monthly", "formats": ["pdf", "csv"]}, headers=customer
    )
    assert r.status_code == 500 and r.json()["error"]["code"] == "REPORT_FAILED"
    assert any(k.endswith(".pdf") for k in stored)  # the PDF had been written before the CSV failed
    new = [i for i in _failed_rows(client, customer) if i["id"] not in before]
    assert len(new) == 1
    failed = new[0]
    assert failed["formats"] == [] and failed["error_message"].startswith("REPORT_FAILED")
    assert client.get(f"{API}/reports/{failed['id']}", headers=customer).json()["formats"] == []
    for fmt in ("pdf", "csv"):
        d = client.get(f"{API}/reports/{failed['id']}/download?format={fmt}", headers=customer)
        assert d.status_code == 404 and d.json()["error"]["code"] == "NOT_FOUND"
        assert d.headers["content-type"].startswith("application/json")
    stored_row = c.repo.get_report(failed["id"])
    assert not any(stored_row.get(f"object_key_{f}") for f in ("pdf", "xlsx", "csv"))


def test_older_failed_rows_with_file_keys_or_raw_errors_are_not_offered(client, customer):
    """Rows written before the fix kept object keys and raw exception text on failure."""
    c = client.app.state.container
    org = client.get(f"{API}/org", headers=customer).json()["id"]
    key = f"exports/reports/test/{new_id('rpt')}.pdf"
    c.objects.put(key, b"%PDF-1.4 partial", "application/pdf")
    rid = new_id("rpt")
    c.repo.insert_report(
        {
            "id": rid,
            "org_id": org,
            "created_by": "test",
            "kind": "scenario_comparison",
            "params": {},
            "status": "failed",
            "created_at": utcnow(),
            "finished_at": utcnow(),
            "object_key_pdf": key,
            "error_message": "ValueError: \nparagraph text '<para><b>Uji</b>' caused exception",
        }
    )
    item = next(
        i for i in client.get(f"{API}/reports?limit=100", headers=customer).json()["items"] if i["id"] == rid
    )
    assert item["formats"] == [] and item["error_message"] == "REPORT_FAILED: Pembuatan laporan gagal."
    d = client.get(f"{API}/reports/{rid}/download?format=pdf", headers=customer)
    assert d.status_code == 404 and b"%PDF" not in d.content


def test_succeeded_report_with_a_missing_file_is_a_clear_error(client, customer, monkeypatch):
    c = client.app.state.container
    r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=customer)
    assert r.status_code == 201 and r.json()["formats"] == ["csv"]
    from icp_api.storage.objects import ObjectNotFound

    def missing(key: str) -> bytes:
        raise ObjectNotFound(key)

    monkeypatch.setattr(c.objects, "get", missing)
    d = client.get(f"{API}/reports/{r.json()['id']}/download?format=csv", headers=customer)
    assert d.status_code == 503 and d.json()["error"]["code"] == "REPORT_OBJECT_MISSING"
    assert d.json()["error"]["request_id"] == d.headers["x-request-id"]


def _fresh_trial(client: Any, tag: str) -> dict[str, str]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Format {tag}",
            "email": f"format-{tag}@format-uji.test",
            "name": "Format",
            "password": "Format-Uji-2026!",
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_a_report_without_any_format_is_rejected_and_not_stored(client, admin):
    """An empty format list used to be stored and listed as a 'succeeded' report with no file (BE-R1)."""
    h = _fresh_trial(client, "empty")
    c = client.app.state.container
    org = client.get(f"{API}/org", headers=h).json()["id"]
    for who in (h, admin):
        r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": []}, headers=who)
        assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR", r.text
    assert client.get(f"{API}/reports", headers=h).json()["items"] == []
    used = c.db.query(
        "SELECT COUNT(*) AS n FROM usage_events WHERE org_id = ? AND action = 'report.generate'", (org,)
    )
    assert used[0]["n"] == 0  # a rejected request is not counted against the plan
    # the service refuses a run without files too (defence in depth for non-HTTP callers)
    from icp_api.errors import ApiError

    try:
        c.reports.generate("data_provenance", {}, "test", org, None, ())
    except ApiError as exc:
        assert exc.status == 422 and exc.code == "VALIDATION_ERROR"
    else:  # pragma: no cover - the defect
        raise AssertionError("a report without formats was generated")
    assert client.get(f"{API}/reports", headers=h).json()["items"] == []


def test_excel_only_request_on_a_plan_without_excel_produces_csv(client):
    """Excel is dropped on plans without it; an Excel-only request then gets CSV, never an empty
    'succeeded' report (BE-R1)."""
    h = _fresh_trial(client, "xlsx-only")
    assert client.get(f"{API}/org", headers=h).json()["entitlements"]["reports_excel"] is False
    r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["xlsx"]}, headers=h)
    assert r.status_code == 201 and r.json()["status"] == "succeeded" and r.json()["formats"] == ["csv"]
    d = client.get(f"{API}/reports/{r.json()['id']}/download?format=csv", headers=h)
    assert d.status_code == 200 and d.headers["content-type"].startswith("text/csv") and "ICP" in d.text
    assert client.get(f"{API}/reports/{r.json()['id']}/download?format=xlsx", headers=h).status_code == 404
