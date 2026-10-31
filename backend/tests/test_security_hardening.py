"""Security hardening v6 (Agent S): adversarial regression tests for report output safety.

Pure tests (no API client) live here; API-level abuse tests live in test_security_api.py.
"""

from __future__ import annotations

import base64
import io
from pathlib import Path
from typing import Any

import pytest

from icp_api.services.reports import cell_safe, harden_reportlab

# 1x1 transparent PNG
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
)


def _build(markup: str) -> None:
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate

    harden_reportlab()
    SimpleDocTemplate(io.BytesIO()).build([Paragraph(markup, getSampleStyleSheet()["BodyText"])])


@pytest.fixture
def png_file(tmp_path: Path) -> Path:
    p = tmp_path / "secret.png"
    p.write_bytes(PNG)
    return p


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every name ReportLab tries to open or fetch (files, UNC shares, URLs)."""
    import builtins

    import reportlab.lib.utils as rl_utils

    calls: list[str] = []
    real_open = builtins.open

    def spy_open(name: Any, *a: Any, **kw: Any) -> Any:
        if isinstance(name, str) and ("secret" in name or name.startswith("\\\\")):
            calls.append(name)
        return real_open(name, *a, **kw)

    monkeypatch.setattr(builtins, "open", spy_open)
    monkeypatch.setattr(rl_utils, "rlUrlRead", lambda name, headers=None: calls.append(name) or PNG)
    return calls


@pytest.mark.parametrize(
    "src",
    [
        "{png}",  # absolute local path to a real image
        "{png_posix}",
        "file:///{png_posix}",
        "data:image/png;base64," + base64.b64encode(PNG).decode(),
        "http://127.0.0.1:9/x.png",
        "https://example.invalid/x.png",
        "//example.invalid/x.png",
        "\\\\attacker.invalid\\share\\secret.png",  # UNC: would leak NTLM credentials on Windows
        "../../secret.png",
    ],
)
def test_pdf_markup_cannot_read_files_or_urls(src: str, png_file: Path, opened: list[str]) -> None:
    """Library-level guard: even raw (unescaped) <img> markup never reaches a file or URL."""
    s = src.format(png=str(png_file), png_posix=png_file.as_posix())
    with pytest.raises(Exception):  # noqa: B017 - ReportLab wraps the refusal in OSError variants
        _build(f'<img src="{s}" width="2" height="2"/>')
    assert opened == []


def test_pdf_links_and_fonts_do_not_fetch(opened: list[str]) -> None:
    # <a href> only writes a link annotation; <font face> with an unknown face is refused or ignored
    for markup in (
        '<a href="http://127.0.0.1:9/x">x</a>',
        '<a href="file:///C:/Windows/win.ini">x</a>',
        '<link href="https://example.invalid">x</link>',
    ):
        _build(markup)
    with pytest.raises(Exception):  # noqa: B017
        _build('<font face="C:/Windows/Fonts/arial.ttf">x</font>')
    assert opened == []


def test_reports_own_chart_image_still_renders() -> None:
    """The guard must not break in-memory images (the report chart is a BytesIO PNG)."""
    from reportlab.platypus import Image, SimpleDocTemplate

    harden_reportlab()
    buf = io.BytesIO()
    SimpleDocTemplate(buf).build([Image(io.BytesIO(PNG), width=20, height=20)])
    assert buf.getvalue().startswith(b"%PDF")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (" =1+2", "' =1+2"),  # leading whitespace is trimmed by spreadsheet importers
        ("  @SUM(A1)", "'  @SUM(A1)"),
        ("\n=1+2", "'\n=1+2"),
        ("\u00a0=1+2", "'\u00a0=1+2"),  # no-break space
        ("\uff1d1+2", "'\uff1d1+2"),  # FULLWIDTH EQUALS SIGN (converted to '=' by some locales)
        ("\uff0bcmd", "'\uff0bcmd"),  # FULLWIDTH PLUS
        ("\uff0dcmd", "'\uff0dcmd"),  # FULLWIDTH HYPHEN-MINUS
        ("\uff20SUM(A1)", "'\uff20SUM(A1)"),  # FULLWIDTH COMMERCIAL AT
        ("\u2212cmd", "'\u2212cmd"),  # MINUS SIGN
        ("=1", "'=1"),
        ("+1", "+1"),  # plain number written as text stays readable
        ("-12,5", "-12,5"),
        ("  -3", "  -3"),
        ("+1+cmd|' /C calc'!A0", "'+1+cmd|' /C calc'!A0"),
        ("-1e5", "'-1e5"),
        ("Brent 80", "Brent 80"),
        (12, 12),
        (None, None),
    ],
)
def test_cell_safe_adversarial(value: Any, expected: Any) -> None:
    assert cell_safe(value) == expected


def test_csv_and_xlsx_outputs_are_inert() -> None:
    """Every text cell of both writers is neutralised; XLSX cells are typed as strings, never formulas."""
    import csv

    from openpyxl import load_workbook

    from icp_api.services.reports import ReportService

    hostile = [" =cmd()", "\t=1", "\uff1d1", "@x", "+cmd", '=HYPERLINK("http://x")']
    rows = [{h: h for h in hostile} | {"n": "12"}]
    content = {
        "title": " =title()",
        "issue_date": "2026-10-20",
        "kpis": [("=k", "=v", "=n")],
        "sections": [("=h", "=t")],
        "tables": {"T": rows},
        "sources": [{"dataset": "=d", "provider": "+p"}],
        "model": {},
    }
    svc = ReportService.__new__(ReportService)
    raw = svc._csv(content).decode("utf-8-sig")
    for row in csv.reader(io.StringIO(raw)):
        for cell in row:
            assert cell in ("n", "12") or cell.startswith("'"), cell
    wb = load_workbook(io.BytesIO(svc._xlsx(content)))
    for ws in wb.worksheets:
        for r in ws.iter_rows():
            for c in r:
                if isinstance(c.value, str):
                    assert c.data_type == "s"
                    assert not c.value.lstrip().startswith(("=", "+", "@", "\uff1d")), c.value
