"""Dashboard contract tests.

A dashboard fails quietly: nothing compiles the SQL or parses the YAML until
someone deploys it, and a typo surfaces in front of the person who asked for
the report. These tests do the parsing up front and check the pieces refer to
each other, so an alert cannot point at a query that was never written or
route to a channel that was never declared.

The dashboard reads the quality metrics the pipeline in notebooks/ writes, so
the two live in one repo and one test run. The last section holds them to
each other: a query may only filter on check types the notebook emits, and
every query is either on a panel or behind an alert.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "dashboard"
LAYOUT = DASHBOARD / "config" / "dashboard_config.json"
QUERIES = sorted((DASHBOARD / "queries").glob("*.sql"))
EXPECTED_QUERY_COUNT = 9
# DDL run by the metric-views job in databricks.yml, not read by a widget.
VIEW_DEFINITIONS = {"08_metric_views"}


@pytest.fixture(scope="module")
def alerts():
    return yaml.safe_load((DASHBOARD / "config" / "alerts.yaml").read_text(encoding="utf-8"))


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_all_documented_queries_exist():
    """The README numbers these 01 to 09. A gap means one was never committed."""
    numbers = sorted(int(q.name.split("_")[0]) for q in QUERIES)
    assert numbers == list(range(1, EXPECTED_QUERY_COUNT + 1)), f"found {numbers}"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_is_non_empty_and_terminated(path):
    body = "\n".join(
        line for line in _sql(path).splitlines() if not line.strip().startswith("--")
    ).strip()
    assert body, f"{path.name} is comments only"
    assert body.endswith(";"), f"{path.name} does not end in a semicolon"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_has_a_purpose_header(path):
    """Every query states why it exists, so the next reader does not guess."""
    assert "Purpose:" in _sql(path)[:600], f"{path.name} has no Purpose header"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_is_fully_qualified(path):
    """Unqualified table names resolve against whatever catalog is current."""
    assert "medallion_demo." in _sql(path), f"{path.name} never names its catalog"


def test_bundle_and_layout_parse():
    """alerts.yaml is parsed by its fixture; these two have no other reader."""
    yaml.safe_load((DASHBOARD / "config" / "databricks.yml").read_text(encoding="utf-8"))
    json.loads(LAYOUT.read_text(encoding="utf-8"))


# --- alerts ----------------------------------------------------------------------


def test_every_alert_points_at_a_real_query(alerts):
    dangling = [a["name"] for a in alerts["alerts"] if not (DASHBOARD / a["query"]).exists()]
    assert not dangling, f"alert references a query that does not exist: {dangling}"


def test_every_alert_names_an_owner_and_a_channel(alerts):
    """An alert with no owner is a notification, and notifications get muted."""
    unowned = [a["name"] for a in alerts["alerts"] if not a.get("owner") or not a.get("channels")]
    assert not unowned, f"alert missing owner or channel: {unowned}"


def test_every_alert_channel_is_declared(alerts):
    declared = set(alerts["channels"])
    unknown = {
        a["name"]: bad
        for a in alerts["alerts"]
        if (bad := [c for c in a["channels"] if c not in declared])
    }
    assert not unknown, f"alert routes to an undeclared channel: {unknown}"


def test_critical_alerts_do_not_route_to_slack_only(alerts):
    """Critical means someone is woken up. A channel message is not that."""
    weak = [
        a["name"]
        for a in alerts["alerts"]
        if a["severity"] == "critical" and set(a["channels"]) == {"slack"}
    ]
    assert not weak, f"critical alert with no durable channel: {weak}"


def test_muted_windows_only_reference_real_alerts(alerts):
    names = {a["name"] for a in alerts["alerts"]}
    bad = [n for window in alerts.get("muted", []) for n in window["alerts"] if n not in names]
    assert not bad, f"mute window names an alert that does not exist: {bad}"


def test_every_alert_runbook_exists(alerts):
    """A runbook link that 404s at 03:00 is worse than no link."""
    missing = [
        a["name"] for a in alerts["alerts"]
        if "runbook" in a and not (DASHBOARD / a["runbook"]).exists()
    ]
    assert not missing, f"alert names a runbook that does not exist: {missing}"


# --- the pipeline and the dashboard agree ---------------------------------------


def _check_type_literals(sql: str) -> set[str]:
    """Every check_type a query filters or pivots on."""
    found: set[str] = set()
    for clause in re.findall(r"check_type\s*(?:=|IN)\s*\(?[^\n)]*", sql):
        found.update(re.findall(r"'([a-z_]+)'", clause))
    return found


def _output_columns(sql: str) -> set[str]:
    """Names a query exposes: aliases, plus bare column lines in its select lists."""
    aliased = re.findall(r"\bAS\s+([a-z_]+)", sql, re.IGNORECASE)
    bare = re.findall(r"^\s+([a-z_]+),?\s*$", sql, re.MULTILINE)
    return set(aliased) | set(bare)


def _widget_datasets() -> set[str]:
    layout = json.loads(LAYOUT.read_text(encoding="utf-8"))
    return {
        q["query"]["datasetName"]
        for page in layout["pages"]
        for item in page["layout"]
        for q in item["widget"].get("queries", [])
    }


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_every_check_type_a_query_reads_is_one_the_pipeline_writes(path, emitted_check_types):
    """A filter on a check_type nothing emits is a panel that is empty forever."""
    unknown = _check_type_literals(_sql(path)) - emitted_check_types
    assert not unknown, (
        f"{path.name} reads check types the quality notebook never writes: {sorted(unknown)}"
    )


def test_freshness_is_recorded_in_minutes(quality_check_functions):
    """Every query, alert and metric view reads the freshness value as minutes."""
    source = quality_check_functions["check_freshness"]
    assert "max_minutes" in source and "/ 60" in source
    assert "3600" not in source


def test_every_alert_metric_is_a_column_of_its_query(alerts):
    """An alert on a column the query does not return never fires."""
    missing = [
        a["name"]
        for a in alerts["alerts"]
        if a["metric"] not in _output_columns(_sql(DASHBOARD / a["query"]))
    ]
    assert not missing, f"alert metric is not a column of its query: {missing}"


def test_every_widget_reads_a_query_that_exists():
    dangling = sorted(d for d in _widget_datasets() if not (DASHBOARD / f"{d}.sql").exists())
    assert not dangling, f"widget reads a dataset with no query file: {dangling}"


def test_every_query_is_shown_or_alerted_on(alerts):
    """A query nobody displays or evaluates is a panel someone forgot to wire."""
    shown = {d.split("/")[-1] for d in _widget_datasets()}
    alerted = {Path(a["query"]).stem for a in alerts["alerts"]}
    orphaned = sorted({q.stem for q in QUERIES} - shown - alerted - VIEW_DEFINITIONS)
    assert not orphaned, f"query is neither on a panel nor behind an alert: {orphaned}"
