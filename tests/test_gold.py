"""Gold layer contract tests.

Gold is what the business reads, so the contract is about shape and lineage.
The expensive mistake here is an aggregate quietly sourced from bronze: it
bypasses every quality expectation in silver while still being presented as a
governed number.
"""

from __future__ import annotations

import pytest

DIMENSIONS = {"dim_customers", "dim_products", "dim_date"}
FACTS = {"fact_orders"}
AGGREGATES = {"agg_daily_revenue", "agg_customer_ltv", "agg_product_performance"}
VALID_PREFIXES = ("dim_", "fact_", "agg_")


def _by_name(gold_tables):
    return {t.name: t for t in gold_tables}


def test_star_schema_is_complete(gold_tables):
    assert {t.name for t in gold_tables} == DIMENSIONS | FACTS | AGGREGATES


def test_table_names_declare_their_role(gold_tables):
    wrong = [t.name for t in gold_tables if not t.name.startswith(VALID_PREFIXES)]
    assert not wrong, f"gold tables must be prefixed dim_, fact_, or agg_: {wrong}"


def test_a_star_schema_has_both_facts_and_dimensions(gold_tables):
    names = {t.name for t in gold_tables}
    assert names & FACTS, "no fact table; this is not a star schema"
    assert len(names & DIMENSIONS) >= 2, "a star needs more than one dimension"


def test_gold_never_reads_bronze(gold_tables):
    """Skipping silver skips every quality expectation."""
    offenders = {
        t.name: [r for r in t.reads if r.startswith("bronze_")]
        for t in gold_tables
        if any(r.startswith("bronze_") for r in t.reads)
    }
    assert not offenders, (
        f"gold must read silver or gold, never bronze directly: {offenders}"
    )


def test_every_gold_read_resolves_to_a_known_table(gold_tables, silver_tables):
    known = {t.name for t in gold_tables} | {t.name for t in silver_tables}
    dangling = {
        t.name: [r for r in t.reads if r not in known]
        for t in gold_tables
        if any(r not in known for r in t.reads)
    }
    assert not dangling, f"gold reads a table nothing declares: {dangling}"


@pytest.mark.parametrize("name", sorted(AGGREGATES))
def test_aggregates_build_on_the_fact_table(gold_tables, name):
    """Aggregates must reuse the conformed fact, not re-derive from silver."""
    table = _by_name(gold_tables)[name]
    assert any(r in FACTS for r in table.reads), (
        f"{name} should aggregate {sorted(FACTS)}, reads {table.reads}"
    )


def test_fact_table_joins_its_dimensions(gold_tables):
    fact = _by_name(gold_tables)["fact_orders"]
    joined = set(fact.reads) & DIMENSIONS
    assert len(joined) >= 2, f"fact_orders joins only {joined}"


def test_date_dimension_is_generated_not_sourced(gold_tables):
    """dim_date is a calendar. Sourcing it from data leaves gaps on quiet days."""
    dim_date = _by_name(gold_tables)["dim_date"]
    assert not dim_date.reads, f"dim_date should be generated, reads {dim_date.reads}"


def test_gold_tables_are_tagged_for_discovery(gold_tables):
    untagged = [t.name for t in gold_tables if not t.properties]
    assert not untagged, f"gold tables need table_properties for the catalog: {untagged}"
