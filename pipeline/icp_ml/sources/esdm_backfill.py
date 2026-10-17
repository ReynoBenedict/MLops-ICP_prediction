"""Resumable backfill of the official ICP series from ESDM (decrees + announcements).

Writes everything under ``<state_dir>`` (default ``var/sources/esdm``):

* ``pdf/<period>_<sha8>.pdf``       source copies of every decree (content-addressed)
* ``decrees.json``                  per-month decree extraction (period, value, method, number, sha)
* ``news_pages.json``               crawl state of the news index (which pages were read)
* ``articles.json``                 parsed monthly announcements
* ``reconciliation.json|csv``       one row per month with status verified/single_source/conflict/missing
* ``run_log.json``                  what was requested, retrieved, added, rejected, unresolved

Every step is idempotent: cached documents are revalidated, parsed results are keyed by URL, and
re-running only does the missing work. Nothing is written into ``data/raw`` (DVC-tracked).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from icp_ml.sources import esdm_icp
from icp_ml.sources.base import HttpClient, SourceError, SourceUnavailable, utcnow_iso

logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATE_DIR = _PROJECT_ROOT / "var" / "sources" / "esdm"


def _load(path: Path, default: Any) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def _save(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)


def _local_mirror_index(mirror_dirs: list[Path]) -> dict[str, Path]:
    """Map period -> previously downloaded decree PDF (e.g. the DVC-tracked data/raw copies)."""
    from icp_ml.ingestion.icp_pdf import _parse_date_from_any_filename

    out: dict[str, Path] = {}
    for d in mirror_dirs:
        for p in sorted(d.glob("*.pdf")) if d.exists() else []:
            period = _parse_date_from_any_filename(p.name)
            if period and period not in out:
                out[period] = p
    return out


def backfill_decrees(
    client: HttpClient, state_dir: Path, limit: int | None = None, mirror_dirs: list[Path] | None = None
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = state_dir / "pdf"
    pdf_dir.mkdir(exist_ok=True)
    decrees: dict[str, Any] = _load(state_dir / "decrees.json", {})
    attempts: dict[str, float] = _load(state_dir / "download_attempts.json", {})
    entries, page_doc = esdm_icp.fetch_archive(client)
    mirror = _local_mirror_index(mirror_dirs or [_PROJECT_ROOT / "data" / "raw"])
    log: dict[str, Any] = {
        "archive_url": esdm_icp.ARCHIVE_URL,
        "archive_sha256": page_doc.sha256,
        "archive_retrieved_at": page_doc.retrieved_at,
        "entries": len(entries),
        "downloaded": 0,
        "cached": 0,
        "from_local_mirror": 0,
        "failed": [],
        "extracted": 0,
        "no_value": [],
    }
    done = 0
    for e in sorted(entries, key=lambda e: e.period):
        rec = decrees.get(e.period)
        if (
            rec
            and rec.get("url") == e.url
            and rec.get("value") is not None
            and rec.get("origin") != "local_mirror"
        ):
            log["cached"] += 1
            continue
        if limit is not None and done >= limit:
            break
        # A month already extracted from the local mirror is re-tried against the archive at most once
        # a day (the ESDM server is slow; a failed download otherwise costs minutes on every run).
        last_try = attempts.get(e.url)
        if rec and rec.get("value") is not None and last_try and (time.time() - last_try) < 86400:
            log["cached"] += 1
            continue
        origin = "download"
        try:
            body, doc = client.get(e.url, max_age_s=365 * 86400)
            sha, nbytes, retrieved = doc.sha256, doc.bytes, doc.retrieved_at
            log["downloaded"] += 0 if doc.from_cache else 1
        except (SourceUnavailable, SourceError) as exc:
            attempts[e.url] = time.time()
            _save(state_dir / "download_attempts.json", attempts)
            if rec and rec.get("value") is not None:
                log["cached"] += 1  # keep the earlier local-mirror extraction when the site is unreachable
                continue
            local_copy = mirror.get(e.period)
            if local_copy is None:
                log["failed"].append({"period": e.period, "url": e.url, "error": str(exc)})
                continue
            body = local_copy.read_bytes()
            from icp_ml.sources.base import sha256_bytes

            sha, nbytes, retrieved, origin = sha256_bytes(body), len(body), utcnow_iso(), "local_mirror"
            log["from_local_mirror"] += 1
            log["failed"].append(
                {"period": e.period, "url": e.url, "error": f"{exc}; used local copy {local_copy.name}"}
            )
        done += 1
        local = pdf_dir / f"{e.period}_{sha[:8]}.pdf"
        if not local.exists():
            local.write_bytes(body)
        ext = esdm_icp.extract_decree(body)
        rec = {
            "period": e.period,
            "url": e.url,
            "sha256": sha,
            "bytes": nbytes,
            "retrieved_at": retrieved,
            "origin": origin,
            "local_path": str(local),
            "extracted_period": ext.period,
            "value": ext.value,
            "method": ext.method,
            "decree_number": ext.decree_number,
            "chars": ext.chars,
            "note": ext.note,
            "period_mismatch": bool(ext.period and ext.period != e.period),
        }
        decrees[e.period] = rec
        if ext.value is None:
            log["no_value"].append(e.period)
        else:
            log["extracted"] += 1
        _save(state_dir / "decrees.json", decrees)
        logger.info("decree %s -> %s (%s)", e.period, ext.value, ext.method)
    _save(state_dir / "decrees.json", decrees)
    return log


def discover_articles(
    client: HttpClient, state_dir: Path, max_pages: int = 110, stop_before_year: int = 2018
) -> dict[str, Any]:
    """Collect ICP announcement links from ESDM's post indexes.

    The news-category index (``kategori=berita``) lists recent announcements and is enough for the
    routine monthly run. A full backfill (``max_pages >= 100``) also walks the unfiltered index,
    where the 2019-2023 announcements are filed, until pages older than ``stop_before_year``.
    """
    state: dict[str, Any] = _load(state_dir / "news_pages.json", {"pages": {}, "links": {}})
    indexes = [("", esdm_icp.NEWS_INDEX_URL + "&page=", max_pages)]
    if max_pages >= 100:
        indexes.append(("all:", esdm_icp.BASE_URL + "/post/index?page=", 400))
    for prefix, base_url, limit in indexes:
        for page in range(1, limit + 1):
            key = f"{prefix}{page}"
            if key in state["pages"]:
                continue
            url = f"{base_url}{page}"
            try:
                body, _ = client.get(url, max_age_s=12 * 3600 if page > 3 else 1800)
            except (SourceUnavailable, SourceError) as exc:
                logger.warning("index page %s failed: %s", key, exc)
                continue
            text = body.decode("utf-8", "replace")
            links = esdm_icp.list_article_links(text)
            years = sorted(
                set(re.findall(r"\d{1,2}\s+(?:" + "|".join(esdm_icp.MONTHS) + r")\s+(20\d\d)", text, re.I))
            )
            for link in links:
                state["links"].setdefault(link, page)
            state["pages"][key] = {"links": len(links), "years": years, "read_at": utcnow_iso()}
            _save(state_dir / "news_pages.json", state)
            logger.info("index page %s: %d ICP links, years %s", key, len(links), years[:1] + years[-1:])
            if years and int(max(years)) < stop_before_year:
                break
    return state


def fetch_articles(client: HttpClient, state_dir: Path, urls: list[str]) -> dict[str, Any]:
    articles: dict[str, Any] = _load(state_dir / "articles.json", {})
    for url in urls:
        if url in articles and articles[url].get("value") is not None and articles[url].get("period"):
            continue  # a value without a month is unusable; re-parse it (the HTML is cached)
        try:
            body, doc = client.get(url, max_age_s=365 * 86400)
        except (SourceUnavailable, SourceError) as exc:
            articles[url] = {"url": url, "error": str(exc)}
            continue
        parsed = esdm_icp.parse_article(url, body.decode("utf-8", "replace"))
        articles[url] = {**asdict(parsed), "sha256": doc.sha256, "retrieved_at": doc.retrieved_at}
        _save(state_dir / "articles.json", articles)
    _save(state_dir / "articles.json", articles)
    return articles


def build_reconciliation(state_dir: Path, legacy_csv: Path | None = None) -> pd.DataFrame:
    decrees = _load(state_dir / "decrees.json", {})
    articles = _load(state_dir / "articles.json", {})
    obs: dict[str, list[dict[str, Any]]] = {}
    for period, d in decrees.items():
        obs.setdefault(period, [])
        if d.get("value") is not None and not d.get("period_mismatch"):
            obs[period].append(
                {
                    "kind": "decree_text" if d["method"] == "text-layer" else "decree_ocr",
                    "value": d["value"],
                    "url": d["url"],
                    "sha256": d["sha256"],
                    "reference": d.get("decree_number"),
                    "note": d.get("note"),
                }
            )
        elif d.get("period_mismatch"):
            obs[period].append(
                {
                    "kind": "decree_mismatch",
                    "value": None,
                    "url": d["url"],
                    "sha256": d["sha256"],
                    "note": f"decree text names {d['extracted_period']} but archive cell is {period}",
                }
            )
    for url, a in articles.items():
        if a.get("period") and a.get("value") is not None:
            obs.setdefault(a["period"], []).append(
                {
                    "kind": "announcement",
                    "value": a["value"],
                    "url": url,
                    "sha256": a.get("sha256"),
                    "reference": a.get("decree_number"),
                    "note": a.get("published"),
                }
            )
        if a.get("previous_period") and a.get("previous_value") is not None:
            obs.setdefault(a["previous_period"], []).append(
                {
                    "kind": "announcement_backref",
                    "value": a["previous_value"],
                    "url": url,
                    "sha256": a.get("sha256"),
                    "note": f"referenced as previous month in {a.get('period')} announcement",
                }
            )
    if legacy_csv and legacy_csv.exists():
        legacy_df = pd.read_csv(legacy_csv)
        for r in legacy_df.itertuples():
            period = f"{int(r.year)}-{int(r.month):02d}"
            obs.setdefault(period, []).append(
                {
                    "kind": "legacy_csv",
                    "value": float(r.icp_price),
                    "url": f"file://{legacy_csv.as_posix()}",
                    "sha256": None,
                    "reference": None,
                    "note": "repository CSV (curated, no document)",
                }
            )
    months = esdm_icp.reconcile(obs)
    df = esdm_icp.to_frame(months)
    if legacy_csv and legacy_csv.exists():
        legacy = pd.read_csv(legacy_csv)
        legacy["period"] = (
            legacy["year"].astype(int).astype(str)
            + "-"
            + legacy["month"].astype(int).astype(str).str.zfill(2)
        )
        df = df.merge(
            legacy[["period", "icp_price"]].rename(columns={"icp_price": "legacy_value"}),
            on="period",
            how="outer",
        ).sort_values("period")
        df["legacy_delta"] = (df["value"] - df["legacy_value"]).round(2)
    df.to_csv(state_dir / "reconciliation.csv", index=False)
    _save(state_dir / "reconciliation.json", [asdict(m) for m in months])
    return df


def run_backfill(
    state_dir: Path = DEFAULT_STATE_DIR,
    client: HttpClient | None = None,
    crawl_news: bool = True,
    legacy_csv: Path | None = None,
) -> dict[str, Any]:
    client = client or HttpClient(min_interval_s=1.5, timeout=(20, 180))
    started = time.time()
    state_dir.mkdir(parents=True, exist_ok=True)
    log: dict[str, Any] = {"started_at": utcnow_iso(), "steps": {}}
    log["steps"]["decrees"] = backfill_decrees(client, state_dir)
    _save(state_dir / "run_log.json", log)
    if crawl_news:
        news = discover_articles(client, state_dir)
        log["steps"]["news_index"] = {"pages_read": len(news["pages"]), "icp_links": len(news["links"])}
        _save(state_dir / "run_log.json", log)
        arts = fetch_articles(client, state_dir, sorted(news["links"]))
        log["steps"]["articles"] = {
            "fetched": len(arts),
            "with_value": sum(1 for a in arts.values() if a.get("value") is not None),
        }
    df = build_reconciliation(state_dir, legacy_csv)
    log["steps"]["reconciliation"] = df["status"].value_counts().to_dict()
    log["finished_at"] = utcnow_iso()
    log["seconds"] = round(time.time() - started)
    _save(state_dir / "run_log.json", log)
    return log


__all__ = [
    "DEFAULT_STATE_DIR",
    "backfill_decrees",
    "build_reconciliation",
    "discover_articles",
    "fetch_articles",
    "run_backfill",
]
