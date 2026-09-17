"""Silver layer contract and quality-rule tests.

Silver is where data earns trust, so the tests here do two things. They assert
every validated table declares expectations, and they run the expectation
predicates themselves against sample rows to prove the rules reject what they
claim to reject. A rule that never fires is worse than no rule, because it
reports a passing quality score.
"""

from __future__ import annotations

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
    return [t for t in silver_tables if t.name not in NO_EXPECTATIONS_EXPECTED]


def test_table_names_are_layer_prefixed(silver_tables):
    wrong = [t.name for t in silver_tables if not t.name.startswith("silver_")]
    assert not wrong, f"silver tables must be prefixed silver_: {wrong}"


def test_every_validated_table_declares_expectations(silver_tables):
    bare = [t.name for t in _validated(silver_tables) if not t.expectations and not t.is_cdc_target]
    assert not bare, f"silver tables must declare quality expectations: {bare}"


def test_silver_reads_only_from_bronze(silver_tables):
    """Silver may not reach into gold, and may not skip bronze."""
    offenders = {
        t.name: [r for r in t.reads if not r.startswith("bronze_")]
        for t in silver_tables
        if any(not r.startswith("bronze_") for r in t.reads)
    }
    assert not offenders, f"silver must read bronze only: {offenders}"


@pytest.mark.parametrize("table_name,keys", CRITICAL_KEYS.items())
def test_primary_keys_are_dropped_not_warned(silver_tables, table_name, keys):
    """A null key must remove the row. Warning on it pollutes every downstream join."""
    table = next((t for t in silver_tables if t.name == table_name), None)
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
    orders = next(t for t in silver_tables if t.name == "silver_orders")
    assert ".dropDuplicates(" in orders.source, (
        "silver_orders must deduplicate; bronze is an at-least-once stream"
    )


def test_customer_dimension_tracks_history(silver_tables):
    customers = next(t for t in silver_tables if t.name == "silver_customers")
    assert customers.is_cdc_target, "silver_customers must be an apply_changes target"


def test_expectation_names_are_unique_per_table(silver_tables):
    for table in silver_tables:
        names = [e.name for e in table.expectations]
        assert len(names) == len(set(names)), f"duplicate expectation name in {table.name}"


# --- The rules themselves, executed ------------------------------------------


@pytest.fixture(scope="module")
def order_rows(spark):
    """One clean row, then one row per rule the silver layer claims to enforce."""
    from datetime import datetime

    return spark.createDataFrame(
        [
            ("o-1", "c-1", 120.00, "confirmed", datetime(2024, 5, 1)),
            (None, "c-2", 80.00, "confirmed", datetime(2024, 5, 2)),   # null key
            ("o-3", "c-3", -5.00, "confirmed", datetime(2024, 5, 3)),  # negative total
            ("o-4", "c-4", 40.00, "teleported", datetime(2024, 5, 4)),  # bad status
            ("o-5", None, 60.00, "shipped", datetime(2024, 5, 5)),      # orphan order
        ],
        "order_id string, customer_id string, order_total double, "
        "order_status string, order_date timestamp",
    )


def _predicate(silver_tables, table_name: str, expectation_name: str) -> str:
    table = next(t for t in silver_tables if t.name == table_name)
    return next(e.predicate for e in table.expectations if e.name == expectation_name)


def test_null_key_rule_removes_the_null_row(spark, silver_tables, order_rows):
    predicate = _predicate(silver_tables, "silver_orders", "valid_order_id")
    kept = {r["order_id"] for r in order_rows.filter(predicate).collect()}
    assert kept == {"o-1", "o-3", "o-4", "o-5"}


def test_positive_amount_rule_removes_the_refund(spark, silver_tables, order_rows):
    predicate = _predicate(silver_tables, "silver_orders", "positive_amount")
    kept = {r["order_id"] for r in order_rows.filter(predicate).collect()}
    assert "o-3" not in kept


def test_status_rule_catches_an_unknown_state(spark, silver_tables, order_rows):
    predicate = _predicate(silver_tables, "silver_orders", "valid_status")
    rejected = {r["order_id"] for r in order_rows.filter(f"NOT ({predicate})").collect()}
    assert rejected == {"o-4"}


def test_every_order_expectation_is_valid_sql(spark, silver_tables, order_rows):
    """A malformed predicate fails the pipeline at runtime, not at review time."""
    orders = next(t for t in silver_tables if t.name == "silver_orders")
    for expectation in orders.expectations:
        order_rows.filter(expectation.predicate).count()


def test_no_order_expectation_is_vacuous(spark, silver_tables, order_rows):
    """Each rule must reject at least one row in the fixture, or it is untested."""
    orders = next(t for t in silver_tables if t.name == "silver_orders")
    total = order_rows.count()
    never_fires = [
        e.name
        for e in orders.expectations
        if e.name != "recent_order" and order_rows.filter(e.predicate).count() == total
    ]
    assert not never_fires, f"expectation never rejects anything in the fixture: {never_fires}"
