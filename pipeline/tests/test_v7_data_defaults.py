"""Offline pipeline defaults use the current EIA-based WTI series (audit v7 DATA-06)."""

from __future__ import annotations

from icp_ml import cli


def test_default_wti_is_the_git_tracked_eia_monthly_mean():
    assert cli.DEFAULT_WTI.parts[-2:] == ("seed", "wti.csv")
    assert cli.DEFAULT_WTI.is_file()
    header = cli.DEFAULT_WTI.read_text(encoding="utf-8").splitlines()[0]
    assert header == "period,wti_price"
