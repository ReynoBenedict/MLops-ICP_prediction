"""Forecast CSV export hardening and explicit tenant checks (coordinator follow-ups to Wave C)."""

from __future__ import annotations

import csv
import io

import pytest

from icp_api.errors import ApiError
from icp_api.security import Principal


def test_listing_without_an_organisation_is_an_explicit_403_not_an_assert():
    from icp_api.routers.forecasts import tenant_org

    with pytest.raises(ApiError) as exc:
        tenant_org(Principal(name="u", role="viewer", kind="user", org_id=None))
    assert exc.value.status == 403 and exc.value.code == "FORBIDDEN"
    assert tenant_org(Principal(name="u", role="viewer", kind="user", org_id="org_1")) == "org_1"


@pytest.mark.parametrize(
    "payload",
    ['=HYPERLINK("http://x")', "+cmd", "@SUM(A1)", chr(0x200B) + "=1+1", "	=1", chr(0xFF1D) + "1"],
)
def test_export_cells_are_neutralised_like_reports(payload):
    from icp_api.routers.forecasts import neutralise_csv

    raw = io.StringIO()
    w = csv.writer(raw, lineterminator="\r\n")
    w.writerow(["period", "step", "prediction_usd", "brent_source"])
    w.writerow(["2026-10", "1", "-3.5", payload])
    out = list(csv.reader(io.StringIO(neutralise_csv(raw.getvalue().encode()).decode("utf-8-sig"))))
    assert out[0] == ["period", "step", "prediction_usd", "brent_source"]
    assert out[1][:3] == ["2026-10", "1", "-3.5"]  # plain numbers stay numbers
    assert out[1][3].startswith("'")


def test_export_endpoint_output_is_neutralised(client, customer, monkeypatch):
    """End to end: a stored export with a formula-like cell is served neutralised."""
    from icp_api.services.forecasts import ForecastService

    stored = b"forecast_id,period,step,brent_source\r\nfc_x,2026-10,1,=EVIL()\r\n"
    row = {"id": "fc_x", "status": "succeeded", "forecast_origin": "2026-09", "result": None, "params": {}}
    monkeypatch.setattr(ForecastService, "export", lambda self, *a, **k: stored)
    monkeypatch.setattr(ForecastService, "get_visible", lambda self, *a, **k: row)
    import icp_api.routers.forecasts as fr

    monkeypatch.setattr(fr, "present_one", lambda request, principal, r: None)
    monkeypatch.setattr(fr, "augment_export", lambda data, run: data)
    r = client.get("/api/v1/forecasts/fc_x/export.csv", headers=customer)
    assert r.status_code == 200, r.text
    assert "'=EVIL()" in r.text and ",=EVIL()" not in r.text
