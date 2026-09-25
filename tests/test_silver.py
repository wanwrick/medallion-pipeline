"""Silver layer contract and quality-rule tests.

Silver is where data earns trust, so the tests here do two things. They assert
every validated table declares expectations, and they run the expectation
predicates themselves against sample rows to prove the rules reject what they
claim to reject. A rule that never fires is worse than no rule, because it
reports a passing quality score.
"""

from __future__ import annotations

from datetime import datetime

import pytest

# Quarantine deliberately carries no expectations: it collects the rows that
# failed them elsewhere. Exempting it keeps the coverage test honest.
NO_EXPECTATIONS_EXPECTED = {"silver_quarantine"}

CRITICAL_KEYS = {
    "silver_orders": ("order_id", "customer_id"),
    "silver_products": ("product_id",),
    "silver_clickstream": ("session_id",),
}


def _validated(silver_tables):
    return [t for t in silver_tables.values() if t.name not in NO_EXPECTATIONS_EXPECTED]


def test_table_names_are_layer_prefixed(silver_tables):
    wrong = [name for name in silver_tables if not name.startswith("silver_")]
    assert not wrong, f"silver tables must be prefixed silver_: {wrong}"


def test_every_validated_table_declares_expectations(silver_tables):
    bare = [t.name for t in _validated(silver_tables) if not t.expectations and not t.is_cdc_target]
    assert not bare, f"silver tables must declare quality expectations: {bare}"


def test_silver_reads_only_from_bronze(silver_tables):
    """Silver may not reach into gold, and may not skip bronze."""
    offenders = {
        t.name: bad
        for t in silver_tables.values()
        if (bad := [r for r in t.reads if not r.startswith("bronze_")])
    }
    assert not offenders, f"silver must read bronze only: {offenders}"


@pytest.mark.parametrize("table_name,keys", CRITICAL_KEYS.items())
def test_primary_keys_are_dropped_not_warned(silver_tables, table_name, keys):
    """A null key must remove the row. Warning on it pollutes every downstream join."""
    table = silver_tables.get(table_name)
    assert table is not None, f"{table_name} not declared"
    for key in keys:
        enforcing = [
            e
            for e in table.expectations
            if key in e.predicate and "NOT NULL" in e.predicate.upper()
        ]
        assert enforcing, f"{table_name}.{key} has no NOT NULL expectation"
        assert any(e.action in {"drop", "fail"} for e in enforcing), (
            f"{table_name}.{key} only warns on null; it must drop or fail"
        )


def test_orders_are_deduplicated(silver_tables):
    assert ".dropDuplicates(" in silver_tables["silver_orders"].source, (
        "silver_orders must deduplicate; bronze is an at-least-once stream"
    )


def test_customer_dimension_tracks_history(silver_tables):
    assert silver_tables["silver_customers"].is_cdc_target, (
        "silver_customers must be an apply_changes target"
    )


def test_expectation_names_are_unique_per_table(silver_tables):
    for table in silver_tables.values():
        names = [e.name for e in table.expectations]
        assert len(names) == len(set(names)), f"duplicate expectation name in {table.name}"


# --- The rules themselves, executed ------------------------------------------


@pytest.fixture(scope="module")
def order_rows(spark):
    """One clean row, then one row per rule the silver layer claims to enforce."""
    return spark.createDataFrame(
        [
            ("o-1", "c-1", 120.00, "confirmed", datetime(2024, 5, 1)),
            (None, "c-2", 80.00, "confirmed", datetime(2024, 5, 2)),   # null key
            ("o-3", "c-3", -5.00, "confirmed", datetime(2024, 5, 3)),  # negative total
            ("o-4", "c-4", 40.00, "teleported", datetime(2024, 5, 4)),  # bad status
            ("o-5", None, 60.00, "shipped", datetime(2024, 5, 5)),      # orphan order
            ("o-6", "c-6", 15.00, "delivered", datetime(2019, 12, 31)),  # stale order
        ],
        "order_id string, customer_id string, order_total double, "
        "order_status string, order_date timestamp",
    )


def _predicate(silver_tables, expectation_name: str) -> str:
    orders = silver_tables["silver_orders"]
    return next(e.predicate for e in orders.expectations if e.name == expectation_name)


def _rejected_by(order_rows, predicate: str) -> set:
    return {r["order_id"] for r in order_rows.filter(f"NOT ({predicate})").collect()}


def test_null_key_rule_rejects_the_null_row(silver_tables, order_rows):
    assert _rejected_by(order_rows, _predicate(silver_tables, "valid_order_id")) == {None}


def test_orphan_rule_rejects_the_order_with_no_customer(silver_tables, order_rows):
    assert _rejected_by(order_rows, _predicate(silver_tables, "valid_customer")) == {"o-5"}


def test_positive_amount_rule_rejects_the_refund(silver_tables, order_rows):
    assert _rejected_by(order_rows, _predicate(silver_tables, "positive_amount")) == {"o-3"}


def test_status_rule_catches_an_unknown_state(silver_tables, order_rows):
    assert _rejected_by(order_rows, _predicate(silver_tables, "valid_status")) == {"o-4"}


def test_recency_rule_flags_the_stale_order(silver_tables, order_rows):
    assert _rejected_by(order_rows, _predicate(silver_tables, "recent_order")) == {"o-6"}


def test_no_order_expectation_is_vacuous(silver_tables, order_rows):
    """Each rule must reject at least one row in the fixture, or it is untested.

    Running every predicate here also proves each one is valid SQL. A malformed
    rule would otherwise fail the pipeline at runtime, not at review time.
    """
    never_fires = [
        e.name
        for e in silver_tables["silver_orders"].expectations
        if not _rejected_by(order_rows, e.predicate)
    ]
    assert not never_fires, f"expectation never rejects anything in the fixture: {never_fires}"
