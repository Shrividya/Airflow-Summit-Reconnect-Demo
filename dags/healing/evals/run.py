from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from healing import evalkit
from healing.evalkit import Case, Outcome


def load_cases(path: Path | None = None) -> list[Case]:
    raw = json.loads((path or Path(__file__).with_name("cases.json")).read_text())
    return [Case(**r) for r in raw]


def build_exception(case: Case) -> BaseException:
    module, _, name = case.exception.rpartition(".")
    try:
        cls = getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError):
        cls = type(name, (Exception,), {"__module__": module})
    if cls.__name__ == "JSONDecodeError":
        return cls(case.message.split(": line")[0], "x" * 16400, 16384)
    try:
        return cls(case.message)
    except TypeError:
        return type(name, (Exception,), {"__module__": module})(case.message)


def make_policy(name: str):
    from healing import smart_retry as sr

    return {
        "rules": sr.rules_policy,
        "llm": lambda: sr.llm_policy(guard=False),
        "classifier": lambda: sr.classifier_policy(guard=False),
        "chain": lambda: sr.rules_then_llm(guard=False),
    }[name]()


def run(policy_name: str, cases: list[Case], repeats: int) -> list[Outcome]:
    policy = make_policy(policy_name)
    outcomes = []
    for case in cases:
        outcome = Outcome(case)
        for _ in range(repeats if policy_name != "rules" else 1):
            try:
                decision = policy.evaluate(build_exception(case), 1, 4, None)
                outcome.decisions.append(evalkit.to_decision(decision.action.value))
            except Exception as exc:  # noqa: BLE001
                print(
                    f"  {policy_name}/{case.id}: policy raised {type(exc).__name__}: {exc}", file=sys.stderr
                )
        outcomes.append(outcome)
    return outcomes


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--policies", default="rules,chain")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)
    cases = load_cases()
    results, detail = {}, {}
    for name in args.policies.split(","):
        outcomes = run(name, cases, args.repeats)
        results[name] = evalkit.score(outcomes)
        detail[name] = {o.case.id: {"label": o.case.label, "decisions": o.decisions} for o in outcomes}
        wrong = evalkit.wrong_stops_on(outcomes)
        unstable = results[name]["unstable_cases"] or "none"
        print(f"{name}: majority-wrong stops on {wrong or 'none'}; unstable: {unstable}")
    print()
    print(evalkit.format_table(results))
    scored = [c for c in cases if c.label != evalkit.DISPUTED]
    print(
        f"\n{len(scored)} scored cases ({sum(c.label == 'retry' for c in scored)} recoverable), "
        f"{len(cases) - len(scored)} disputed and ignored, {args.repeats} repeats per model call."
    )
    if args.out:
        Path(args.out).write_text(json.dumps({"results": results, "detail": detail}, indent=2, default=list))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
