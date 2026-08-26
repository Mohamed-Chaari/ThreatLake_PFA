"""Natural-language question -> raw SQL text, via an LLM. This module never
executes anything and never trusts its own output - the caller (the
/copilot/query route) always runs the result through
threatlake.copilot.guardrails.validate_and_bound before touching Spark.

PROVIDER: Gemini, called directly over HTTP (``requests``, already a
project dependency - see threatlake.enrichment.reputation for the same
pattern) rather than pulling in a dedicated SDK, to keep this a small,
auditable HTTP call rather than a new dependency surface. The provider is
a config value (``settings.copilot.provider``) specifically so a
different one can be swapped in later without touching the guardrail or
API layers - today only "gemini" is implemented.

API KEY: read from the ``GEMINI_API_KEY`` environment variable at call
time, never from YAML or Settings - see CopilotSettings' own docstring.
Get one at https://aistudio.google.com/app/apikey (free tier is enough
for this).
"""

from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING

import requests

from threatlake.copilot.prompts import build_system_prompt

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

__all__ = [
    "GEMINI_API_KEY_ENV_VAR",
    "CopilotConfigError",
    "CopilotProviderError",
    "generate_sql",
]

_LOG = logging.getLogger(__name__)

GEMINI_API_KEY_ENV_VAR = "GEMINI_API_KEY"
_GEMINI_URL_TEMPLATE = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
)
#: 30s was too tight even with thinkingConfig.thinkingBudget=0 disabling
#: the reasoning pass below - real, observed latency against the live API
#: (repeated direct calls, thinking disabled) still landed in the 55-70s
#: range. 90s gives real headroom above that observed ceiling rather than
#: a number picked to just barely clear one lucky measurement.
_REQUEST_TIMEOUT_SECONDS = 90

_SQL_FENCE_PATTERN = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


class CopilotConfigError(RuntimeError):
    """The copilot isn't configured to run at all (e.g. no API key) - not
    a rejection of any particular question, every question fails the same
    way until this is fixed.
    """


class CopilotProviderError(RuntimeError):
    """The LLM provider call itself failed (network, timeout, malformed
    response) - distinct from a guardrail rejection, which means the
    provider DID respond and its answer was rejected on inspection.
    """


def _extract_sql(raw_text: str) -> str:
    """Pull SQL out of the model's response - strips a ```sql fenced block
    if present (models reliably add one despite being told not to),
    otherwise trusts the whole response is SQL.
    """
    text = raw_text.strip()
    match = _SQL_FENCE_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return text


def _call_gemini(system_prompt: str, question: str, settings: Settings, api_key: str) -> str:
    # The key goes in a header, never a query param: requests' own
    # exception messages (and anything that logs a failed request's URL)
    # include the full URL - a query-string key would leak into error
    # messages, logs, and (via CopilotProviderError) potentially back to
    # the API caller. The header never appears in any of those.
    url = _GEMINI_URL_TEMPLATE.format(model=settings.copilot.model)
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    payload = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": question}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": settings.copilot.max_output_tokens,
            # Confirmed empirically: "gemini-flash-latest" currently resolves
            # to a reasoning ("thinking") model variant, which - especially
            # against this endpoint's longer, rule-heavy system prompt - can
            # spend 30-90+ seconds on internal reasoning before answering a
            # question that has exactly one correct, mechanical SQL
            # translation. thinkingBudget=0 disables that reasoning pass;
            # consistent with temperature=0 above, this is a deterministic
            # translation task, not one that benefits from deliberation.
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    try:
        response = requests.post(
            url, headers=headers, json=payload, timeout=_REQUEST_TIMEOUT_SECONDS
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise CopilotProviderError(f"Gemini API call failed: {_safe_str(exc)}") from exc

    data = response.json()
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, TypeError) as exc:
        raise CopilotProviderError(f"Unexpected Gemini response shape: {data!r}") from exc
    return str(text)


def _safe_str(exc: requests.RequestException) -> str:
    """A defense-in-depth second layer on top of the header-based auth
    above: even if a future change reintroduces a key in the URL (e.g. a
    redirect Gemini issues), never let it reach a log line or an API
    response.
    """
    text = str(exc)
    return re.sub(r"([?&]key=)[^&\s]+", r"\1REDACTED", text)


def generate_sql(question: str, spark: SparkSession, settings: Settings) -> str:
    """Return raw, UNVALIDATED SQL text generated for ``question``.

    Callers MUST pass this through
    threatlake.copilot.guardrails.validate_and_bound before execution -
    this function makes no safety guarantees about its own output, by
    design: generation and validation are two separate concerns, and
    conflating them is exactly how a "the model said it was safe" bug
    happens.
    """
    if settings.copilot.provider != "gemini":
        raise CopilotConfigError(f"Unsupported copilot provider {settings.copilot.provider!r}")

    api_key = os.environ.get(GEMINI_API_KEY_ENV_VAR)
    if not api_key:
        raise CopilotConfigError(
            f"{GEMINI_API_KEY_ENV_VAR} is not set - get a free key at "
            "https://aistudio.google.com/app/apikey and export it before starting uvicorn"
        )

    system_prompt = build_system_prompt(spark, settings)
    raw_text = _call_gemini(system_prompt, question, settings, api_key)
    sql = _extract_sql(raw_text)
    _LOG.info("copilot generated SQL for question=%r: %s", question, sql)
    return sql
