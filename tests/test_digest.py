from healing.digest import build_digest
from healing.links import task_url
from healing.routing import Severity, classify, dispatch, format_message

ABSORBED = {
    "refresh_fx_rates": ("resolve_fx_rates", "cached FX rates used"),
    "parse_settlements": ("triage_settlements", "settlements quarantined"),
}
BASE = "http://af.example"


def url_for(dag_id, run_id, task_id, try_number=None):
    return task_url(dag_id, run_id, task_id, try_number, base_url=BASE)


def digest(states, attempts=None, notes=None, summary=""):
    return build_digest(
        dag_id="d",
        run_id="r1",
        states=states,
        attempts=attempts or {},
        absorbed=ABSORBED,
        notes=notes,
        summary=summary,
        url_for=url_for,
    )


def sections(event):
    out = {}
    for i in event.items:
        out.setdefault(i.section, []).append(i.task_id)
    return out


def test_clean_run_is_info_and_stays_out_of_slack():
    e = digest({"a": "success", "b": "success"})
    assert classify(e) is Severity.INFO and e.items == ()
    sent = []
    dispatch(e, {"log": lambda *_: sent.append("log"), "slack": lambda *_: sent.append("slack")})
    assert sent == ["log"]


def test_task_that_healed_on_retry_is_listed_with_attempt_number_and_that_attempts_log():
    e = digest({"fetch": "success"}, attempts={"fetch": {"attempt": 2, "max": 5}})
    (item,) = e.items
    assert (item.section, item.text) == ("recovered", "succeeded on attempt 2/5")
    assert item.url == f"{BASE}/dags/d/runs/r1/tasks/fetch?try_number=2"
    assert classify(e) is Severity.WARNING


def test_failure_absorbed_by_its_resolver_is_handled_not_attention():
    e = digest(
        {"refresh_fx_rates": "failed", "resolve_fx_rates": "success"},
    )
    assert sections(e) == {"handled": ["refresh_fx_rates"]}
    assert "cached FX rates used" in e.items[0].text
    assert classify(e) is Severity.WARNING


def test_dag_supplied_note_overrides_the_default():
    e = digest(
        {"parse_settlements": "failed", "triage_settlements": "success"},
        notes={"parse_settlements": "settlements healed"},
    )
    assert "settlements healed" in e.items[0].text


def test_failure_whose_resolver_also_failed_needs_attention_with_a_link():
    e = digest({"refresh_fx_rates": "failed", "resolve_fx_rates": "failed", "load": "upstream_failed"})
    assert set(sections(e)["attention"]) == {"load", "refresh_fx_rates", "resolve_fx_rates"}
    assert classify(e) is Severity.CRITICAL
    assert all(i.url.startswith(BASE) for i in e.items)


def test_unabsorbed_failure_needs_attention_and_is_listed_first():
    e = digest(
        {"load_warehouse": "failed", "fetch": "success"},
        attempts={"fetch": {"attempt": 2, "max": 5}},
    )
    assert [i.section for i in e.items] == ["attention", "recovered"]
    assert e.items[0].url == f"{BASE}/dags/d/runs/r1/tasks/load_warehouse"


def test_attempts_of_tasks_that_did_not_end_in_success_are_ignored():
    e = digest({"x": "failed"}, attempts={"x": {"attempt": 3, "max": 5}})
    assert sections(e) == {"attention": ["x"]}


def test_plain_text_rendering_has_sections_links_and_summary():
    e = digest(
        {"load_warehouse": "failed", "fetch": "success"},
        attempts={"fetch": {"attempt": 2, "max": 5}},
        summary="Load failed.",
    )
    text = format_message(e)
    assert "needs attention" in text and "Needs attention:" in text and "Healed on retry:" in text
    assert "succeeded on attempt 2/5" in text and f"{BASE}/dags/d/runs/r1/tasks/load_warehouse" in text
    assert "Summary: Load failed." in text


def test_task_url_quotes_run_ids_and_trims_trailing_slash():
    url = task_url("d", "manual__2026-10-31T18:30:06+00:00", "t", base_url="http://x/")
    assert url == "http://x/dags/d/runs/manual__2026-10-31T18%3A30%3A06%2B00%3A00/tasks/t"
