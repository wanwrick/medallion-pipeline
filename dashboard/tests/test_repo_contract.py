"""Repo contract tests.

A dashboard repo fails quietly: the SQL is never compiled, the YAML is never
parsed, and a typo surfaces the first time someone deploys it. These tests do
the parsing up front, and check that the pieces actually refer to each other.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
QUERIES = sorted((ROOT / "queries").glob("*.sql"))
EXPECTED_QUERY_COUNT = 8


def test_all_documented_queries_exist():
    """The README numbers these 01 to 08. A gap means one was never committed."""
    numbers = sorted(int(q.name.split("_")[0]) for q in QUERIES)
    assert numbers == list(range(1, EXPECTED_QUERY_COUNT + 1)), f"found {numbers}"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_is_non_empty_and_terminated(path):
    body = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("--")
    ).strip()
    assert body, f"{path.name} is comments only"
    assert body.endswith(";"), f"{path.name} does not end in a semicolon"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_has_a_purpose_header(path):
    """Every query states why it exists, so the next reader does not guess."""
    head = path.read_text(encoding="utf-8")[:600]
    assert "Purpose:" in head, f"{path.name} has no Purpose header"


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_is_fully_qualified(path):
    """Unqualified table names resolve against whatever catalog is current."""
    sql = path.read_text(encoding="utf-8")
    assert "medallion_demo." in sql, f"{path.name} never names its catalog"


def test_configs_parse():
    yaml.safe_load((ROOT / "config" / "alerts.yaml").read_text(encoding="utf-8"))
    yaml.safe_load((ROOT / "config" / "databricks.yml").read_text(encoding="utf-8"))
    json.loads((ROOT / "config" / "dashboard_config.json").read_text(encoding="utf-8"))


def _alerts():
    return yaml.safe_load((ROOT / "config" / "alerts.yaml").read_text(encoding="utf-8"))


def test_every_alert_points_at_a_real_query():
    dangling = [a["name"] for a in _alerts()["alerts"] if not (ROOT / a["query"]).exists()]
    assert not dangling, f"alert references a query that does not exist: {dangling}"


def test_every_alert_names_an_owner_and_a_channel():
    """An alert with no owner is a notification, and notifications get muted."""
    unowned = [
        a["name"] for a in _alerts()["alerts"] if not a.get("owner") or not a.get("channels")
    ]
    assert not unowned, f"alert missing owner or channel: {unowned}"


def test_every_alert_channel_is_declared():
    config = _alerts()
    declared = set(config["channels"])
    unknown = {
        a["name"]: [c for c in a["channels"] if c not in declared]
        for a in config["alerts"]
        if any(c not in declared for c in a["channels"])
    }
    assert not unknown, f"alert routes to an undeclared channel: {unknown}"


def test_critical_alerts_do_not_route_to_slack_only():
    """Critical means someone is woken up. A channel message is not that."""
    weak = [
        a["name"]
        for a in _alerts()["alerts"]
        if a["severity"] == "critical" and set(a["channels"]) == {"slack"}
    ]
    assert not weak, f"critical alert with no durable channel: {weak}"


def test_muted_windows_only_reference_real_alerts():
    config = _alerts()
    names = {a["name"] for a in config["alerts"]}
    bad = [
        n for window in config.get("muted", []) for n in window["alerts"] if n not in names
    ]
    assert not bad, f"mute window names an alert that does not exist: {bad}"
