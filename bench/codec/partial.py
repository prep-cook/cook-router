#!/usr/bin/env python3
"""Diagnostic codec comparison over the workloads of named protocols only.

Not acceptance evidence: bench/codec-gate.sh requires every workload and fails a harness
that covers fewer protocols. This runs the same driver functions (manifest, describe,
alternating rounds, records, noise band, thresholds) on the workloads whose client and
upstream protocols are both listed, so merged codecs can be measured before the rest.

With `--ok-only PREVIOUS_OUT`, each workload keeps only the cases the new codec translated
successfully in that earlier run (bench/codec/outcomes.py), and workloads left with no case
are dropped, so both sides are timed over the same successful translations. The noise band
still validates against the full manifest; its allowances were calibrated on all cases.

usage: CODEC_NOISE_BAND=... partial.py --protocols openai,claude [--ok-only PREVIOUS_OUT] OLD_CODEC NEW_CODEC OUT
"""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("gate", HERE / "gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
spec = importlib.util.spec_from_file_location("outcomes", HERE / "outcomes.py")
outcomes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(outcomes)


def identities(descriptions):
    old, new = descriptions["old"], descriptions["new"]
    if old["executable_sha256"] == new["executable_sha256"]:
        raise ValueError("old/new executable sha256 is identical")
    if all(old[field] == new[field] for field in ("revision", "source_checksum")):
        raise ValueError("old/new revision and source checksum are identical")
    if old["revision"] != gate.BASELINE_REVISION or new["revision"] == gate.BASELINE_REVISION:
        raise ValueError(f"old must be, and new must not be, {gate.BASELINE_REVISION}")
    if old["config"] != new["config"]:
        raise ValueError("old/new configuration differs")


def matrix(out, summary, protocols, ok_only):
    """Validate the band against the full manifest, then measure the selected subset."""
    full = gate.manifest()
    gate.save(out / "workloads-full.json", full)
    rows = [row for row in full["workloads"]
            if row["client_protocol"] in protocols and row["upstream_protocol"] in protocols]
    if ok_only:
        ok = outcomes.ok_cases(Path(ok_only))
        if missing := {row["id"] for row in rows} - set(ok):
            raise ValueError(f"{ok_only} has no outcomes for {len(missing)} selected workloads")
        rows = [{**row, "cases": [case for case in row["cases"] if case["index"] in ok[row["id"]]]}
                for row in rows]
        rows = [row for row in rows if row["cases"]]
        summary["ok_only_from"] = str(Path(ok_only).resolve())
    if not rows:
        raise ValueError(f"no workload uses only {sorted(protocols)}")
    data = {**full, "workloads": rows}
    path = out / "workloads.json"
    gate.save(path, data)
    summary.update({"mode": "partial", "acceptance_evidence": False, "protocols": sorted(protocols),
                    "full_manifest_sha256": gate.digest(out / "workloads-full.json"),
                    "full_workload_count": len(full["workloads"]), "workload_count": len(rows),
                    "manifest_sha256": gate.digest(path), "driver_sha256": gate.digest(HERE / "gate.py"),
                    "partial_driver_sha256": gate.digest(Path(__file__)),
                    "fixture_pins_sha256": gate.digest(HERE / "fixtures.json")})
    return full, data, path


def run(protocols, ok_only, old, new, out):
    out = gate.prepare(out)
    summary = {"schema_version": 1, "status": "error", "rounds": []}
    try:
        executables = {"old": Path(old).resolve(strict=True), "new": Path(new).resolve(strict=True)}
        descriptions = {label: gate.describe(path, out, label) for label, path in executables.items()}
        summary["harnesses"] = descriptions
        identities(descriptions)
        full, data, path = matrix(out, summary, protocols, ok_only)
        band_path = os.environ.get("CODEC_NOISE_BAND")
        band, allowances = gate.load_band(band_path, descriptions["old"], summary["full_manifest_sha256"],
                                          {row["id"] for row in full["workloads"]})
        summary["noise_band"] = {"path": str(Path(band_path).resolve()), "sha256": gate.digest(Path(band_path)),
                                 "created_utc": band["created_utc"]}
        checksum, required = summary["manifest_sha256"], {row["id"] for row in data["workloads"]}
        storage = [out, *(Path(value["storage_directory"]) for value in descriptions.values())]
        summary["host"] = gate.snapshot(storage)
        rounds = {"old": [], "new": []}
        for round_number in range(1, gate.WARMUP_ROUNDS + gate.GATE_ROUNDS + 1):
            for label in ("old", "new"):
                values = gate.measure(executables[label], descriptions[label], label, round_number,
                                      path, checksum, required, out, storage, summary)
                if round_number > gate.WARMUP_ROUNDS:
                    rounds[label].append(values)
                else:
                    summary["rounds"][-1]["discarded_warmup"] = True
        comparisons = gate.compare(data["workloads"], rounds, allowances)
        summary["comparisons"] = comparisons
        summary["failed_metrics"] = sum(not row["pass"] for row in comparisons)
        summary["status"] = "partial-fail" if summary["failed_metrics"] else "partial-pass"
        return 1 if summary["failed_metrics"] else 0
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        summary["error"] = str(error)
        return 2
    finally:
        gate.save(out / "summary.json", summary)
        print(f"codec partial (not acceptance evidence): {summary['status']}; {out / 'summary.json'}")


def main():
    args = sys.argv[1:]
    ok_only = None
    if len(args) == 7 and args[2] == "--ok-only":
        ok_only = args.pop(3)
        args.pop(2)
    if len(args) != 5 or args[0] != "--protocols":
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    try:
        return run(set(filter(None, args[1].split(","))), ok_only, *args[2:])
    except (OSError, ValueError) as error:
        print(f"codec partial: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
