"""AST-based validation for copilot-generated SQL, run BEFORE any query
touches Spark - see docs/adr/0005-copilot-gold-only-guardrail-first.md.

Why AST-based, not regex/keyword-matching: a regex over the raw SQL string
cannot correctly distinguish "DROP appears inside an executable statement"
from "DROP appears inside a comment or a string literal" - it either
over-rejects safe queries that happen to mention a banned word in a
comment, or (worse) under-rejects a real attack that uses whitespace,
casing, or comment tricks a regex didn't anticipate. Parsing with sqlglot
and inspecting the resulting expression tree answers the actual question
this module cares about: what will Spark SQL actually EXECUTE, not what
substrings appear in the text. ``_raw_keyword_scan`` below is a deliberate
exception to that - see its own docstring for why a comment mentioning a
banned keyword is rejected anyway, on top of the AST checks.

Every rejection is raised as :class:`GuardrailRejectionError` with a specific,
honest reason - the API layer logs it and returns it to the caller
verbatim, never a generic "invalid query".
"""
 

from __future__ import annotations

import logging
import re

import sqlglot
from sqlglot import exp

__all__ = ["ALLOWED_TABLES", "MAX_ROWS", "GuardrailRejectionError", "validate_and_bound"]

_LOG = logging.getLogger(__name__)

#: The only tables the copilot may ever read. Bronze, silver, and ml_scores
#: are deliberately excluded - a smaller, safer surface given the
#: timeline, not an oversight. Every other module that needs this exact
#: set (prompts.py's schema description, the /copilot/query router's temp
#: view registration) imports it from here rather than redeclaring it, so
#: there is exactly one place the allowlist can be edited.
#:
#: ThreatLake AI (the full project this is a subset of) allows 5 gold
#: tables here. PFA only ever builds 2 (see
#: threatlake.transform.gold) - trimmed to match, since prompts.py reads
#: every allowed table's live schema at request time and would fail on a
#: table that doesn't exist.
ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        "attacker_profiles",
        "attack_timeline",
    }
)

#: Server-side row cap, enforced regardless of what the generated SQL asks
#: for - a query with no LIMIT, or a LIMIT larger than this, is rewritten
#: to exactly this value before execution.
MAX_ROWS = 200

#: Expression types sqlglot can produce at the top level of a parsed
#: statement that are genuinely read-only. Anything else (Insert, Update,
#: Delete, Drop, Alter, Create, TruncateTable, Grant, Command, ...) is
#: rejected by construction - this is an allowlist of what's PERMITTED,
#: not a blocklist of what's forbidden, so a statement type this module's
#: author didn't think to name explicitly is rejected by default rather
#: than accepted by default.
_READ_ONLY_TYPES = (exp.Select, exp.Union, exp.Intersect, exp.Except)

#: Belt-and-suspenders raw scan (see module docstring): these must not
#: appear anywhere in the input text at all, including inside a comment -
#: a legitimate analytics question about the 5 gold tables never needs to
#: mention any of them, so their presence anywhere is itself suspicious
#: regardless of whether a real parser would treat it as inert.
_BANNED_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "EXEC",
    "EXECUTE",
    "CALL",
)
_BANNED_KEYWORD_PATTERN = re.compile(r"\b(" + "|".join(_BANNED_KEYWORDS) + r")\b", re.IGNORECASE)


class GuardrailRejectionError(Exception):
    """Raised with a specific, user-facing reason - never a bare message."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _raw_keyword_scan(sql: str) -> None:
    match = _BANNED_KEYWORD_PATTERN.search(sql)
    if match:
        raise GuardrailRejectionError(
            f"the generated SQL contains the word {match.group(1).upper()!r}, which is "
            "never allowed anywhere in a copilot query - not even inside a comment"
        )


def _parse_single_statement(sql: str) -> exp.Expression:
    try:
        # isinstance, not `is not None`: sqlglot.parse's own declared return
        # type is the broader `Expr | None`, but every real parsed
        # statement is an `Expression` (the subclass this module's checks
        # rely on - .find_all, .args, .set) - narrowing via isinstance
        # keeps mypy honest about that instead of asserting it away.
        statements = [s for s in sqlglot.parse(sql, read="spark") if isinstance(s, exp.Expression)]
    except Exception as exc:  # sqlglot raises its own ParseError subclasses
        raise GuardrailRejectionError(f"could not parse as SQL: {exc}") from exc

    if len(statements) == 0:
        raise GuardrailRejectionError("empty query")
    if len(statements) > 1:
        raise GuardrailRejectionError(
            f"expected exactly one SQL statement, found {len(statements)} - "
            "multi-statement execution is not allowed"
        )
    return statements[0]


def _check_read_only(statement: exp.Expression) -> None:
    if not isinstance(statement, _READ_ONLY_TYPES):
        raise GuardrailRejectionError(
            f"only SELECT queries are allowed - the generated statement was a "
            f"{type(statement).__name__.upper()}"
        )


def _check_table_allowlist(statement: exp.Expression) -> None:
    for table in statement.find_all(exp.Table):
        if table.catalog or table.db:
            raise GuardrailRejectionError(
                f"qualified table reference {table.sql()!r} is not allowed - only "
                "bare gold-table names may be queried (no catalog or schema prefix)"
            )
        name = table.name.lower()
        if name not in ALLOWED_TABLES:
            raise GuardrailRejectionError(
                f"table {name!r} is not queryable by the copilot - only "
                f"{', '.join(sorted(ALLOWED_TABLES))} are allowed"
            )


def _requested_row_limit(statement: exp.Expression) -> int | None:
    """The row count the query asked for, or None when it did not ask for a
    usable one.

    None covers three cases that all get treated identically by the caller:
    no LIMIT clause at all, a LIMIT whose expression is not a plain integer
    literal (e.g. an expression or a parameter), and a malformed node. None
    means "no opinion from the query", NOT "zero rows".
    """
    existing = statement.args.get("limit")
    if existing is None:
        return None
    try:
        return int(existing.expression.this)
    except (AttributeError, TypeError, ValueError):
        return None


def _bound_row_limit(statement: exp.Expression, max_rows: int) -> exp.Expression:
    """Rewrite the statement's LIMIT to a value never above ``max_rows``.

    The cap is applied by REWRITING the query, not by checking it and
    rejecting: a query asking for a million rows is a reasonable question
    answered with the first ``max_rows`` of them, not an attack.
    """
    requested_rows = _requested_row_limit(statement)

    if requested_rows is None:
        # Query expressed no usable limit - impose the server-side cap.
        bounded_rows = max_rows
    elif requested_rows <= 0:
        # LIMIT 0 / negative: not a meaningful request from an analyst tool,
        # so fall back to the cap rather than returning an empty result.
        bounded_rows = max_rows
    else:
        bounded_rows = min(requested_rows, max_rows)

    statement.set("limit", exp.Limit(expression=exp.Literal.number(bounded_rows)))
    return statement


def validate_and_bound(sql: str, *, max_rows: int = MAX_ROWS) -> str:
    """Validate ``sql`` and return a rewritten, row-capped version safe to
    execute - or raise :class:`GuardrailRejectionError` with a specific reason.

    Order matters: the raw keyword scan runs first (cheapest check, and
    catches the "banned word in a comment" case a parser would otherwise
    consider inert), then parsing, then the structural AST checks, then
    the row-limit rewrite. Nothing here executes the query - that's the
    caller's job, only after this function returns successfully.
    """
    sql = sql.strip()
    if not sql:
        raise GuardrailRejectionError("empty query")

    _raw_keyword_scan(sql)
    statement = _parse_single_statement(sql)
    _check_read_only(statement)
    _check_table_allowlist(statement)
    bounded = _bound_row_limit(statement, max_rows)

    return bounded.sql(dialect="spark")
