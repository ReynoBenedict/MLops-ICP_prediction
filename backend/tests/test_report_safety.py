"""Report output safety: PDF markup injection / SSRF, spreadsheet formula injection, error leakage.

Scenario names are free text chosen by any trial user; before 2026-10-19 they reached ReportLab
markup unescaped (``<img src=...>`` made the server fetch URLs) and spreadsheet cells unneutralised.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pytest

from icp_api.services.reports import cell_safe, esc

API = "/api/v1"
PW = "Laporan-Aman-2026!"
SSRF_NAME = '<img src="http://127.0.0.1:9/ssrf.png" width="2" height="2"/>'
BROKEN_MARKUP = "Uji <font size=40>besar & <b"
FORMULA_NAME = '=HYPERLINK("https://example.invalid","klik")'


# -- pure helpers ------------------------------------------------------------------------------
def test_esc_neutralises_paragraph_markup():
    assert esc(SSRF_NAME).startswith("&lt;img src=&quot;http")
    assert "<" not in esc(BROKEN_MARKUP) and esc("A & B") == "A &amp; B"
    assert esc(12.5) == "12.5"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("=1+2", "'=1+2"),
        ("+cmd|' /C calc'!A0", "'+cmd|' /C calc'!A0"),
        ("-2+3+cmd", "'-2+3+cmd"),
        ("@SUM(A1)", "'@SUM(A1)"),
        ("\tx", "'\tx"),
        ("\rx", "'\rx"),
        ("+26.1%", "+26.1%"),  # a plain signed number stays readable
        ("-1.234,5", "-1.234,5"),
        ("-", "-"),
        ("Skenario A", "Skenario A"),
        ("", ""),
        (-3.5, -3.5),
        (None, None),
    ],
)
def test_cell_safe(value: Any, expected: Any):
    assert cell_safe(value) == expected


def test_reportlab_refuses_remote_and_file_resources(monkeypatch):
    """Defence in depth behind escaping: even raw markup cannot fetch a URL."""
    import reportlab.lib.utils as rl_utils
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate

    import icp_api.services.reports  # noqa: F401  (applies harden_reportlab at import)

    calls: list[str] = []
    monkeypatch.setattr(rl_utils, "rlUrlRead", lambda name: calls.append(name) or b"")
    doc = SimpleDocTemplate(io.BytesIO())
    with pytest.raises(Exception):  # noqa: B017 - ReportLab raises OSError/ValueError variants
        doc.build([Paragraph(SSRF_NAME, getSampleStyleSheet()["BodyText"])])
    assert calls == []


# -- end to end through the API -----------------------------------------------------------------
@pytest.fixture(scope="module")
def pro(client: Any, trained_version: str) -> dict[str, Any]:
    operator = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "report safety tests", "force": True},
        headers=operator,
    )
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Laporan Aman",
            "email": "owner@laporan-aman.test",
            "name": "Owner",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    org = r.json()["organization"]["id"]
    up = client.post(
        f"{API}/admin/organizations/{org}/plan",
        json={"plan": "professional", "reason": "report safety tests"},
        headers=operator,
    )
    assert up.status_code == 200, up.text
    return {"h": {"Authorization": f"Bearer {r.json()['token']}"}, "org": org}


def _scenario(client: Any, h: dict[str, str], name: str) -> str:
    r = client.post(
        f"{API}/scenarios",
        json={"name": name, "horizon": 2, "inputs": {"brent": {}}},
        headers=h,
    )
    assert r.status_code == 201, r.text
    return str(r.json()["id"])


def test_hostile_scenario_names_give_valid_reports_without_fetching(client, pro, monkeypatch):
    import reportlab.lib.utils as rl_utils

    calls: list[str] = []
    monkeypatch.setattr(rl_utils, "rlUrlRead", lambda name: calls.append(name) or b"")
    ids = [_scenario(client, pro["h"], n) for n in (SSRF_NAME, BROKEN_MARKUP, FORMULA_NAME)]
    r = client.post(
        f"{API}/reports",
        json={
            "kind": "scenario_comparison",
            "params": {"scenario_ids": ids},
            "formats": ["pdf", "xlsx", "csv"],
        },
        headers=pro["h"],
    )
    assert r.status_code == 201, r.text
    assert calls == []
    rid = r.json()["id"]
    pdf = client.get(f"{API}/reports/{rid}/download?format=pdf", headers=pro["h"])
    assert pdf.status_code == 200 and pdf.content.startswith(b"%PDF")

    raw = client.get(f"{API}/reports/{rid}/download?format=csv", headers=pro["h"]).content.decode("utf-8-sig")
    header = next(csv.reader(io.StringIO(raw)))
    formula_cols = [h for h in header if "HYPERLINK" in h]
    assert formula_cols and all(h.startswith("'=") for h in formula_cols)
    assert not any(h.startswith(("=", "+", "@")) for h in header)

    from openpyxl import load_workbook

    xb = client.get(f"{API}/reports/{rid}/download?format=xlsx", headers=pro["h"]).content
    wb = load_workbook(io.BytesIO(xb))
    texts = [c for ws in wb.worksheets for row in ws.iter_rows() for c in row if isinstance(c.value, str)]
    assert texts and all(c.data_type == "s" for c in texts)
    assert not any(str(c.value).startswith("=") for c in texts)
    assert any(str(c.value).startswith("'=HYPERLINK") for c in texts)


def test_unexpected_report_failure_does_not_leak_exception_text(client, pro, monkeypatch):
    from icp_api.services import reports as reports_mod

    def boom(self: Any, c: dict[str, Any]) -> bytes:
        raise RuntimeError("secret internal path C:/srv/icp/var/objects at 0xdeadbeef")

    monkeypatch.setattr(reports_mod.ReportService, "_csv", boom)
    r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=pro["h"])
    assert r.status_code == 500 and r.json()["error"]["code"] == "REPORT_FAILED"
    failed = [
        i for i in client.get(f"{API}/reports", headers=pro["h"]).json()["items"] if i["status"] == "failed"
    ]
    assert failed
    msg = failed[0]["error_message"]
    assert (
        msg.startswith("REPORT_FAILED")
        and "secret" not in msg
        and "0x" not in msg
        and "RuntimeError" not in msg
    )


def test_rejected_report_requests_leave_no_failed_rows(client, pro):
    """A foreign or unknown forecast id is a rejected request, not a report run: 404 and nothing stored."""
    before = client.get(f"{API}/reports", headers=pro["h"]).json()["items"]
    r = client.post(
        f"{API}/reports",
        json={"kind": "forecast", "params": {"forecast_id": "fc_does_not_exist"}, "formats": ["csv"]},
        headers=pro["h"],
    )
    assert r.status_code == 404
    after = client.get(f"{API}/reports", headers=pro["h"]).json()["items"]
    assert len(after) == len(before)
