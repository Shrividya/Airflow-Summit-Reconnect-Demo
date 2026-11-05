from __future__ import annotations

import json
import json.decoder
import logging
import os
from datetime import timedelta

from airflow.providers.common.compat.sdk import ChainRetryPolicy
from airflow.sdk import ExceptionRetryPolicy, RetryAction, RetryDecision, RetryPolicy, RetryRule

from .errors import RateLimited, SchemaDrift, Unauthorized, WarehouseSuspended

log = logging.getLogger(__name__)

RULES = [
    RetryRule(
        exception=RateLimited,
        action=RetryAction.RETRY,
        retry_delay=timedelta(seconds=20),
        reason="rate limited: back off longer than usual",
    ),
    RetryRule(
        exception=Unauthorized,
        action=RetryAction.FAIL,
        reason="credentials rejected: retrying will not help",
    ),
    RetryRule(
        exception=SchemaDrift,
        action=RetryAction.FAIL,
        reason="schema drift: the file will not change on retry, hand over to the healing agent",
    ),
    RetryRule(
        exception=WarehouseSuspended,
        action=RetryAction.RETRY,
        retry_delay=timedelta(seconds=10),
        reason="warehouse suspended: it auto-resumes, so wait and retry",
    ),
    RetryRule(
        exception=[json.decoder.JSONDecodeError, UnicodeDecodeError],
        action=RetryAction.RETRY,
        retry_delay=timedelta(seconds=10),
        reason="decode error: usually a truncated or in-flight read, which a retry fixes",
    ),
]

_DEMO_NOTE = (
    "\n\nThis is a live demo. When you decide to retry, keep suggested_delay_seconds between 5 and 20."
)


def rules_policy() -> ExceptionRetryPolicy:
    return ExceptionRetryPolicy(rules=RULES)


class GuardedModelPolicy(RetryPolicy):

    def __init__(self, inner: RetryPolicy, name: str, retry_only: bool | None = None) -> None:
        self._inner = inner
        self._name = name
        self._retry_only = retry_only

    @property
    def retry_only(self) -> bool:
        if self._retry_only is not None:
            return self._retry_only
        return os.getenv("HEALING_RETRY_ONLY", "true").strip().lower() not in {"0", "false", "no"}

    def evaluate(self, exception, try_number, max_tries, context=None) -> RetryDecision:
        decision = self._inner.evaluate(exception, try_number, max_tries, context)
        would_stop = decision.action is RetryAction.FAIL
        final = decision
        if would_stop and self.retry_only:
            final = RetryDecision.retry(
                delay=decision.retry_delay, reason=f"retry-only mode, model wanted to stop: {decision.reason}"
            )
        log.info("healing_decision %s", decision_record(self._name, exception, try_number, decision, final))
        return final


def decision_record(policy: str, exception: BaseException, try_number: int, decision, final) -> str:
    cls = type(exception)
    return json.dumps(
        {
            "policy": policy,
            "exception": f"{cls.__module__}.{cls.__qualname__}",
            "message": str(exception)[:300],
            "try_number": try_number,
            "model_action": decision.action.value,
            "model_delay_s": decision.retry_delay.total_seconds() if decision.retry_delay else None,
            "model_reason": decision.reason,
            "would_stop": decision.action is RetryAction.FAIL,
            "applied_action": final.action.value,
        },
        sort_keys=True,
    )


def llm_policy(llm_conn_id: str = "pydanticai_default", fallback_rules=None, guard: bool = True):
    from airflow.providers.common.ai.policies.retry import (
        DEFAULT_INSTRUCTIONS,
        LLMRetryPolicy,
    )

    kwargs = {}
    if isinstance(DEFAULT_INSTRUCTIONS, str):
        kwargs["instructions"] = DEFAULT_INSTRUCTIONS + _DEMO_NOTE
    policy = LLMRetryPolicy(
        llm_conn_id=llm_conn_id,
        timeout=30.0,
        max_exception_length=2048,
        fallback_rules=fallback_rules,
        **kwargs,
    )
    return GuardedModelPolicy(policy, "llm") if guard else policy


def classifier_policy(
    llm_conn_id: str = "pydanticai_default",
    min_confidence: float = 0.7,
    fallback_policy=None,
    guard: bool = True,
):
    from airflow.providers.common.ai.policies.retry import (
        DEFAULT_CATEGORIES,
        ClassifierRetryPolicy,
        ErrorCategory,
    )

    quick = {
        name: ErrorCategory(
            c.description,
            retry=c.retry,
            delay=timedelta(seconds=10) if c.delay else None,
        )
        for name, c in DEFAULT_CATEGORIES.items()
    }
    policy = ClassifierRetryPolicy(
        llm_conn_id=llm_conn_id,
        categories=quick,
        min_confidence=min_confidence,
        fallback_policy=fallback_policy,
        timeout=30.0,
        max_exception_length=2048,
    )
    return GuardedModelPolicy(policy, "classifier") if guard else policy


def rules_then_llm(llm_conn_id: str = "pydanticai_default", guard: bool = True) -> ChainRetryPolicy:
    return ChainRetryPolicy([rules_policy(), llm_policy(llm_conn_id, fallback_rules=RULES, guard=guard)])
