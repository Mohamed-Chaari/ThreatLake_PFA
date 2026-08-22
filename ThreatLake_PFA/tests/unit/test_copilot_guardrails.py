"""threatlake.copilot.guardrails.validate_and_bound - the ONLY thing that
decides whether copilot-generated SQL is allowed to run. Copied
unmodified from ThreatLake AI except ALLOWED_TABLES, which is trimmed
from 5 gold tables to PFA's 2 - see that module's own docstring."""

from __future__ import annotations

import pytest

from threatlake.copilot.guardrails import (
    ALLOWED_TABLES,
    MAX_ROWS,
    GuardrailRejectionError,
    validate_and_bound,
)


def test_allowed_tables_is_trimmed_to_the_two_pfa_gold_tables() -> None:
    assert ALLOWED_TABLES == frozenset({"attacker_profiles", "attack_timeline"})


def test_valid_select_is_accepted_and_passed_through() -> None:
    sql = validate_and_bound("SELECT src_ip, total_events FROM attacker_profiles LIMIT 10")
    assert "attacker_profiles" in sql
    assert "LIMIT 10" in sql


def test_empty_query_is_rejected() -> None:
    with pytest.raises(GuardrailRejectionError, match="empty query"):
        validate_and_bound("   ")


def test_non_select_statement_is_rejected() -> None:
    # EXPLAIN, not DELETE/DROP/etc: those are caught earlier by the raw
    # keyword scan (see the parametrized test below) - this exercises the
    # SEPARATE structural "only SELECT/UNION/INTERSECT/EXCEPT" AST check.
    with pytest.raises(GuardrailRejectionError, match="only SELECT"):
        validate_and_bound("EXPLAIN SELECT * FROM attacker_profiles")


def test_unlisted_table_is_rejected() -> None:
    with pytest.raises(GuardrailRejectionError, match="not queryable"):
        validate_and_bound("SELECT * FROM ml_scores")


def test_qualified_table_reference_is_rejected() -> None:
    with pytest.raises(GuardrailRejectionError, match="not allowed"):
        validate_and_bound("SELECT * FROM some_schema.attacker_profiles")


@pytest.mark.parametrize("keyword", ["DROP", "DELETE"])
def test_banned_keyword_is_rejected_even_inside_a_comment(keyword: str) -> None:
    sql = f"SELECT * FROM attacker_profiles -- {keyword} TABLE attacker_profiles"
    with pytest.raises(GuardrailRejectionError, match=keyword):
        validate_and_bound(sql)


def test_multiple_statements_are_rejected() -> None:
    with pytest.raises(GuardrailRejectionError, match="exactly one"):
        validate_and_bound("SELECT * FROM attacker_profiles; SELECT * FROM attack_timeline")


def test_row_limit_is_capped_at_max_rows() -> None:
    sql = validate_and_bound(f"SELECT * FROM attacker_profiles LIMIT {MAX_ROWS * 10}")
    assert f"LIMIT {MAX_ROWS}" in sql


def test_missing_limit_gets_the_server_side_cap_applied() -> None:
    sql = validate_and_bound("SELECT * FROM attacker_profiles")
    assert f"LIMIT {MAX_ROWS}" in sql


def test_a_reasonable_limit_is_left_untouched() -> None:
    sql = validate_and_bound("SELECT * FROM attacker_profiles LIMIT 5")
    assert "LIMIT 5" in sql


def test_unparseable_sql_is_rejected() -> None:
    with pytest.raises(GuardrailRejectionError, match="could not parse"):
        validate_and_bound("SELECT FROM WHERE ((( not sql")
