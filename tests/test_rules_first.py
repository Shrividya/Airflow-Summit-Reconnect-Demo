import pytest

pytest.importorskip("airflow")

from airflow.providers.common.compat.sdk import ChainRetryPolicy  # noqa: E402
from airflow.sdk import RetryAction, RetryDecision, RetryPolicy  # noqa: E402
from healing.errors import RateLimited, Unauthorized, WarehouseSuspended  # noqa: E402
from healing.smart_retry import GuardedModelPolicy, rules_policy  # noqa: E402


class Spy(RetryPolicy):

    def __init__(self):
        self.calls = 0

    def evaluate(self, exception, try_number, max_tries, context=None):
        self.calls += 1
        return RetryDecision.fail("model says no")


def decide(exc):
    spy = Spy()
    return ChainRetryPolicy([rules_policy(), spy]).evaluate(exc, 1, 4), spy


def test_suspended_warehouse_is_retried_and_the_model_is_never_asked():
    decision, spy = decide(WarehouseSuspended("suspended"))
    assert decision.action is RetryAction.RETRY and decision.retry_delay.total_seconds() == 10
    assert spy.calls == 0


@pytest.mark.parametrize(
    "exc,action", [(RateLimited("429"), RetryAction.RETRY), (Unauthorized("401"), RetryAction.FAIL)]
)
def test_other_known_errors_are_also_final(exc, action):
    decision, spy = decide(exc)
    assert decision.action is action and spy.calls == 0


def test_decode_errors_are_retried_by_rule_so_the_model_cannot_stop_on_them():
    import json

    for exc in (
        json.JSONDecodeError("Expecting value", "x", 0),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"),
    ):
        decision, spy = decide(exc)
        assert decision.action is RetryAction.RETRY and spy.calls == 0


def test_schema_drift_fails_by_rule_so_the_agent_runs_instead_of_retries():
    from healing.errors import SchemaDrift

    decision, spy = decide(SchemaDrift("expected column 'amount', found 'amount_cents'"))
    assert decision.action is RetryAction.FAIL and spy.calls == 0


def test_unknown_errors_go_to_the_model():
    decision, spy = decide(ConnectionError("503 from warehouse"))
    assert spy.calls == 1 and decision.action is RetryAction.FAIL


def test_a_stopping_model_is_downgraded_to_a_retry_by_default_but_the_stop_is_logged(caplog):
    import logging

    class Stopper(RetryPolicy):
        def evaluate(self, exception, try_number, max_tries, context=None):
            return RetryDecision.fail("permanent")

    guarded = GuardedModelPolicy(Stopper(), "test", retry_only=True)
    with caplog.at_level(logging.INFO):
        decision = guarded.evaluate(ValueError("x"), 1, 4)
    assert decision.action is RetryAction.RETRY
    line = next(r.getMessage() for r in caplog.records if "healing_decision" in r.getMessage())
    assert '"would_stop": true' in line and '"applied_action": "retry"' in line


def test_with_retry_only_off_the_model_may_stop_and_the_delay_is_kept():
    class Decider(RetryPolicy):
        def __init__(self, decision):
            self.decision = decision

        def evaluate(self, exception, try_number, max_tries, context=None):
            return self.decision

    from datetime import timedelta

    stop = GuardedModelPolicy(Decider(RetryDecision.fail("p")), "t", retry_only=False)
    assert stop.evaluate(ValueError(), 1, 4).action is RetryAction.FAIL
    waits = GuardedModelPolicy(
        Decider(
            RetryDecision.fail(
                "p",
            )
        ),
        "t",
        retry_only=True,
    )
    assert waits.evaluate(ValueError(), 1, 4).retry_delay is None
    delayed = RetryDecision(action=RetryAction.FAIL, retry_delay=timedelta(seconds=7), reason="p")
    assert GuardedModelPolicy(Decider(delayed), "t", retry_only=True).evaluate(
        ValueError(), 1, 4
    ).retry_delay == timedelta(seconds=7)


def test_env_switch_controls_retry_only(monkeypatch):
    policy = GuardedModelPolicy(Spy(), "t")
    monkeypatch.delenv("HEALING_RETRY_ONLY", raising=False)
    assert policy.retry_only is True
    monkeypatch.setenv("HEALING_RETRY_ONLY", "false")
    assert policy.retry_only is False
