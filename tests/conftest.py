"""Shared fixtures for the medallion pipeline test suite.

DLT notebooks cannot be imported outside a Databricks runtime, so these tests
read the notebook source and inspect it with `ast`. That keeps the suite
runnable on any laptop and in CI, and it still catches the failures that
actually happen: a layer reaching across a boundary, a table shipped without
quality expectations, a missing lineage column.

The Spark fixture is optional. Tests that need it skip when no local Spark is
available rather than failing the run.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = REPO_ROOT / "notebooks"
CONFIG_DIR = REPO_ROOT / "config"

BRONZE_NOTEBOOK = "01_bronze_ingestion.py"
SILVER_NOTEBOOK = "02_silver_transformation.py"
GOLD_NOTEBOOK = "03_gold_aggregation.py"
QUALITY_NOTEBOOK = "04_data_quality_checks.py"


@dataclass
class Expectation:
    """A single `@dlt.expect*` declaration."""

    name: str
    predicate: str
    action: str  # "warn", "drop", or "fail"


@dataclass
class TableDef:
    """A table declared in a DLT notebook, however it was declared."""

    name: str
    properties: dict[str, str] = field(default_factory=dict)
    expectations: list[Expectation] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)
    source: str = ""
    is_cdc_target: bool = False


_EXPECT_ACTIONS = {
    "expect": "warn",
    "expect_or_drop": "drop",
    "expect_or_fail": "fail",
    "expect_all": "warn",
    "expect_all_or_drop": "drop",
    "expect_all_or_fail": "fail",
}


def _literal(node: ast.AST):
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def _kwarg(call: ast.Call, key: str):
    for kw in call.keywords:
        if kw.arg == key:
            return _literal(kw.value)
    return None


def _dotted_name(node: ast.AST) -> str:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _collect_reads(node: ast.AST) -> list[str]:
    """Every `dlt.read` / `dlt.read_stream` target inside a node."""
    reads: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        fn = _dotted_name(child.func)
        if fn in {"dlt.read", "dlt.read_stream"} and child.args:
            target = _literal(child.args[0])
            if isinstance(target, str):
                reads.append(target)
    return reads


def parse_notebook(filename: str) -> dict[str, TableDef]:
    """Return every table a DLT notebook declares, keyed by name.

    Handles both forms in use here: the `@dlt.table` decorator on a function,
    and the `dlt.create_streaming_table` plus `dlt.apply_changes` pair used for
    the CDC dimension.
    """
    path = NOTEBOOK_DIR / filename
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    tables: dict[str, TableDef] = {}

    for node in tree.body:
        # Form 1: decorated function.
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            table_call = None
            expectations: list[Expectation] = []
            for dec in node.decorator_list:
                if not isinstance(dec, ast.Call):
                    continue
                fn = _dotted_name(dec.func)
                if fn == "dlt.table":
                    table_call = dec
                elif fn.startswith("dlt.expect"):
                    action = _EXPECT_ACTIONS.get(fn.split(".", 1)[1], "warn")
                    if len(dec.args) >= 2:
                        name, predicate = _literal(dec.args[0]), _literal(dec.args[1])
                        if isinstance(name, str) and isinstance(predicate, str):
                            expectations.append(Expectation(name, predicate, action))
            if table_call is None:
                continue
            name = _kwarg(table_call, "name") or node.name
            tables[name] = TableDef(
                name=name,
                properties=_kwarg(table_call, "table_properties") or {},
                expectations=expectations,
                reads=_collect_reads(node),
                source=ast.get_source_segment(source, node) or "",
            )

        # Form 2: create_streaming_table / apply_changes at module level.
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            fn = _dotted_name(call.func)
            if fn == "dlt.create_streaming_table":
                name = _kwarg(call, "name")
                if isinstance(name, str):
                    tables.setdefault(name, TableDef(name=name))
                    tables[name].properties = _kwarg(call, "table_properties") or {}
            elif fn == "dlt.apply_changes":
                target = _kwarg(call, "target")
                sources = _kwarg(call, "source")
                if isinstance(target, str):
                    tbl = tables.setdefault(target, TableDef(name=target))
                    tbl.is_cdc_target = True
                    if isinstance(sources, str):
                        tbl.reads.append(sources)

    return tables


@pytest.fixture(scope="session")
def bronze_tables() -> dict[str, TableDef]:
    return parse_notebook(BRONZE_NOTEBOOK)


@pytest.fixture(scope="session")
def silver_tables() -> dict[str, TableDef]:
    return parse_notebook(SILVER_NOTEBOOK)


@pytest.fixture(scope="session")
def gold_tables() -> dict[str, TableDef]:
    return parse_notebook(GOLD_NOTEBOOK)


def _quality_source() -> str:
    return (NOTEBOOK_DIR / QUALITY_NOTEBOOK).read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def emitted_check_types() -> set[str]:
    """Every check_type literal the quality notebook writes to data_quality_metrics."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(_quality_source())):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Constant)
                and key.value == "check_type"
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                found.add(value.value)
    return found


@pytest.fixture(scope="session")
def quality_check_functions() -> dict[str, str]:
    """Source of each check_* function in the quality notebook, by name."""
    source = _quality_source()
    return {
        node.name: ast.get_source_segment(source, node) or ""
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name.startswith("check_")
    }


@pytest.fixture(scope="session")
def spark():
    """Local Spark session, or skip. Requires a JVM, which CI may not have."""
    pyspark = pytest.importorskip("pyspark", reason="pyspark is not installed")
    try:
        session = (
            pyspark.sql.SparkSession.builder.master("local[1]")
            .appName("medallion-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # no JVM on the box
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()
