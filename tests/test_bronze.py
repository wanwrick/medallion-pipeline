"""Bronze layer contract tests.

Bronze has one job: land the source faithfully and record where it came from.
The failure mode worth guarding against is a well-meaning filter or cleanup
creeping into the raw layer, because once bronze drops a row, no downstream
layer can recover it.
"""

from __future__ import annotations

import pytest

LINEAGE_COLUMNS = ("_ingestion_timestamp", "_source_file")
EXPECTED_SOURCES = {"bronze_orders", "bronze_customers", "bronze_products", "bronze_clickstream"}


def test_all_expected_sources_are_declared(bronze_tables):
    assert {t.name for t in bronze_tables} == EXPECTED_SOURCES


def test_table_names_are_layer_prefixed(bronze_tables):
    wrong = [t.name for t in bronze_tables if not t.name.startswith("bronze_")]
    assert not wrong, f"bronze tables must be prefixed bronze_: {wrong}"


@pytest.mark.parametrize("column", LINEAGE_COLUMNS)
def test_every_table_stamps_lineage(bronze_tables, column):
    """Without these, a bad batch cannot be traced back to its file."""
    missing = [t.name for t in bronze_tables if column not in t.source]
    assert not missing, f"{column} missing from: {missing}"


def test_quality_tier_is_tagged(bronze_tables):
    wrong = [
        t.name for t in bronze_tables if t.properties.get("quality") != "bronze"
    ]
    assert not wrong, f"table_properties quality must be 'bronze': {wrong}"


def test_bronze_does_not_filter_rows(bronze_tables):
    """Raw means raw. Filtering belongs in silver, where it is expectation-tracked."""
    offenders = [
        t.name
        for t in bronze_tables
        if any(op in t.source for op in (".filter(", ".where(", ".dropDuplicates("))
    ]
    assert not offenders, (
        "bronze must not drop rows; move this to silver so the loss is measured: "
        f"{offenders}"
    )


def test_bronze_reads_no_other_pipeline_table(bronze_tables):
    """Bronze is the entry point. Reading another DLT table would be a cycle."""
    offenders = {t.name: t.reads for t in bronze_tables if t.reads}
    assert not offenders, f"bronze tables must read from source storage only: {offenders}"


def test_schema_location_is_set_for_auto_loader(bronze_tables):
    """Auto Loader without a schema location silently re-infers on every restart."""
    missing = [
        t.name
        for t in bronze_tables
        if "cloudFiles" in t.source and "cloudFiles.schemaLocation" not in t.source
    ]
    assert not missing, f"Auto Loader needs cloudFiles.schemaLocation: {missing}"


def test_no_hardcoded_credentials(bronze_tables):
    banned = ("password=", "secret=", "api_key=", "AKIA", "dapi")
    hits = [
        (t.name, token)
        for t in bronze_tables
        for token in banned
        if token in t.source
    ]
    assert not hits, f"possible credential in notebook source: {hits}"
