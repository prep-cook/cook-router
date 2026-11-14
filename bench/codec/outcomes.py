#!/usr/bin/env python3
"""Classify each fixture case's new-codec outcome from a gate or partial run's evidence.

The new harness records, per workload, each case's outcome on its first measured iteration
(`ok`, `invalid_input`, `unsupported` or `other_error`, mapped in bench/codec/CONTRACT.md),
with the refusing stage and codec error. The outcome is the codec's verdict, not ground truth:
a codec stricter than the real API reports valid input as `invalid_input`. A stream case
appears in one workload per wire kind and counts once.

usage: outcomes.py OUT   (OUT holds workloads.json and new-N-evidence/ from codec-gate.sh or partial.py)
"""
from collections import Counter
import json
from pathlib import Path
import sys

OUTCOMES = ("ok", "invalid_input", "unsupported", "other_error")
EXAMPLES = 4


def rounds(out):
    """Per warm round: workload id -> evidence `cases` list."""
    result = []
    for directory in sorted(out.glob("new-[2-9]-evidence")):
        result.append({path.stem: json.loads(path.read_text())["cases"] for path in directory.glob("*.json")})
    if not result:
        raise ValueError(f"{out} holds no warm new-side evidence")
    if any(other != result[0] for other in result[1:]):
        raise ValueError("case outcomes differ between warm rounds")
    return result[0]


def ok_cases(out):
    """Workload id -> fixture indexes whose translation succeeded."""
    return {workload: {case["index"] for case in cases if case["outcome"] == "ok"}
            for workload, cases in rounds(out).items()}


def classify(out):
    """Unique (pair, role, fixture id) -> outcome record."""
    rows = {row["id"]: row for row in json.loads((out / "workloads.json").read_text())["workloads"]}
    cases = {}
    for workload, outcomes in rounds(out).items():
        row = rows[workload]
        name = Path(row["fixture"]).name
        for case in outcomes:
            key = (row["protocol"], row["direction"], f"{name}#{case['index']}")
            previous = cases.setdefault(key, case)
            if previous != case:
                raise ValueError(f"{key[2]} {key[1]} has different outcomes across its workloads")
    return cases


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    cases = classify(Path(sys.argv[1]))
    totals = Counter((pair, role, case["outcome"]) for (pair, role, _), case in cases.items())
    print("| Pair | Role | Cases | " + " | ".join(OUTCOMES) + " |")
    print("| --- | --- | ---: |" + " ---: |" * len(OUTCOMES))
    for pair, role in sorted({(pair, role) for pair, role, _ in totals}):
        counts = [totals[(pair, role, outcome)] for outcome in OUTCOMES]
        print(f"| {pair} | {role} | {sum(counts)} | " + " | ".join(map(str, counts)) + " |")
    counts = [sum(n for (_, _, o), n in totals.items() if o == outcome) for outcome in OUTCOMES]
    print(f"| all | all | {sum(counts)} | " + " | ".join(map(str, counts)) + " |\n")
    causes = {}
    for (pair, role, fixture), case in sorted(cases.items()):
        if case["outcome"] != "ok":
            causes.setdefault((pair, role, case["stage"], case["error"], case["outcome"]), []).append(fixture)
    print("| Pair | Role | Stage | Error | Outcome | Cases | Examples |")
    print("| --- | --- | --- | --- | --- | ---: | --- |")
    for (pair, role, stage, error, outcome), fixtures in sorted(causes.items(), key=lambda kv: -len(kv[1])):
        examples = ", ".join(f"`{f}`" for f in fixtures[:EXAMPLES])
        print(f"| {pair} | {role} | {stage} | {error} | {outcome} | {len(fixtures)} | {examples} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
