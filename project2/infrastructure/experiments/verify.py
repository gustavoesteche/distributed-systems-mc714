#!/usr/bin/env python3
"""Check the correctness invariants of one experiment result directory.

This is a reproducible log/state check, complementing the Go tests: it reads
the final /debug/state of every node and verifies causal delivery, contiguous
counters, observed removes, reconstructed quantities and cluster convergence.
Exit code 0 means every invariant held.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def compare(a: dict, b: dict) -> str:
    """Return Equal/Before/After/Concurrent for two vector clocks."""
    keys = set(a) | set(b)
    less = any(a.get(k, 0) < b.get(k, 0) for k in keys)
    greater = any(a.get(k, 0) > b.get(k, 0) for k in keys)
    if less and greater:
        return "Concurrent"
    if less:
        return "Before"
    if greater:
        return "After"
    return "Equal"


def check_state(path: Path) -> tuple[dict, list[str]]:
    errors: list[str] = []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not data:
        return {}, [f"{path.name}: empty state"]

    ops = data.get("ops") or []
    pending = data.get("pending") or []
    if pending:
        errors.append(f"{path.name}: {len(pending)} pending operations")

    by_origin: dict[str, list[dict]] = {}
    by_counter: dict[str, dict[int, tuple[dict, int]]] = {}
    by_id: dict[str, dict] = {}
    index: dict[str, int] = {}
    for i, op in enumerate(ops):
        oid = op.get("id")
        origin = op.get("origin")
        counter = op.get("counter")
        if not oid or not origin or not isinstance(counter, int):
            errors.append(f"{path.name}: malformed op at index {i}: {op}")
            continue
        if oid in index:
            errors.append(f"{path.name}: duplicate op {oid}")
        index[oid] = i
        by_id[oid] = op
        by_origin.setdefault(origin, []).append(op)
        by_counter.setdefault(origin, {})[counter] = (op, i)
        if op.get("clock", {}).get(origin) != counter:
            errors.append(f"{path.name}: {oid} clock[origin] != counter")
        if oid != f"{origin}:{counter}":
            errors.append(f"{path.name}: {oid} does not match origin/counter")

    for origin, list_ops in by_origin.items():
        counters = sorted(op["counter"] for op in list_ops)
        if counters != list(range(1, len(counters) + 1)):
            errors.append(f"{path.name}: origin {origin} counters are not contiguous: {counters[:20]}")

    for op in ops:
        oid = op["id"]
        origin = op["origin"]
        here = index[oid]
        for pred_origin, need in (op.get("clock") or {}).items():
            if pred_origin == origin:
                required = need - 1
            else:
                required = need
            if required <= 0:
                continue
            found = by_counter.get(pred_origin, {}).get(required)
            if found is None:
                errors.append(
                    f"{path.name}: {oid} needs op {required} from {pred_origin}, which is missing")
                continue
            pred, pred_index = found
            if pred_index >= here:
                errors.append(
                    f"{path.name}: causal predecessor {pred['id']} is after {oid}")

        if op.get("type") == "REMOVE":
            removes = op.get("removes") or []
            if len(removes) != op.get("quantity"):
                errors.append(f"{path.name}: {oid} removes {len(removes)} for quantity {op.get('quantity')}")
            if len(set(removes)) != len(removes):
                errors.append(f"{path.name}: {oid} names a unit twice")
            for unit in removes:
                try:
                    add_id, unit_index = unit.rsplit("/", 1)
                    unit_index = int(unit_index)
                except ValueError:
                    errors.append(f"{path.name}: {oid} has malformed unit {unit!r}")
                    continue
                add = by_id.get(add_id)
                if add is None:
                    errors.append(f"{path.name}: {oid} removes unknown unit {unit}")
                    continue
                if index[add_id] >= here:
                    errors.append(f"{path.name}: {oid} removes unit from a later ADD {add_id}")
                if add.get("type") != "ADD" or not 0 <= unit_index < add.get("quantity", 0):
                    errors.append(f"{path.name}: {oid} removes invalid unit {unit}")
                order = compare(add.get("clock") or {}, op.get("clock") or {})
                if order not in ("Before", "Equal"):
                    errors.append(f"{path.name}: {oid} removes unit {unit} from an unobserved ADD")

    # Reconstruct quantities exactly as the replica does.
    live: dict[str, set[str]] = {}
    tombstones: set[str] = set()
    for op in ops:
        if op.get("type") == "ADD":
            product = op["product_id"]
            units = live.setdefault(product, set())
            for i in range(op["quantity"]):
                unit = f"{op['id']}/{i}"
                if unit not in tombstones:
                    units.add(unit)
        elif op.get("type") == "REMOVE":
            product = op["product_id"]
            for unit in op.get("removes") or []:
                tombstones.add(unit)
                live.setdefault(product, set()).discard(unit)
    reconstructed = {p: len(units) for p, units in live.items() if units}
    if reconstructed != (data.get("quantities") or {}):
        errors.append(f"{path.name}: quantities mismatch: state={data.get('quantities')} reconstructed={reconstructed}")
    if set(data.get("tombstones") or []) != tombstones:
        errors.append(f"{path.name}: tombstone set does not match reconstructed removes")

    expected_clock: dict[str, int] = {}
    for origin, list_ops in by_origin.items():
        expected_clock[origin] = len(list_ops)
    if data.get("clock") != expected_clock:
        errors.append(f"{path.name}: clock mismatch: state={data.get('clock')} expected={expected_clock}")

    return data, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()

    states = sorted(args.run_dir.glob("state-node-*.json"))
    if not states:
        print(json.dumps({"ok": False, "errors": ["no state-node-*.json files"]}, indent=2))
        return 1

    all_errors: list[str] = []
    parsed = {}
    for path in states:
        data, errors = check_state(path)
        parsed[path.name] = data
        all_errors.extend(errors)

    summary_path = args.run_dir / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if not summary.get("convergence", {}).get("converged", False):
            all_errors.append("summary reports convergence.converged=false")
    else:
        all_errors.append("summary.json is missing")

    if len(parsed) > 1:
        names = sorted(parsed)
        first_name = names[0]
        first = parsed[first_name]
        first_ids = sorted(op["id"] for op in first.get("ops") or [])
        for name in names[1:]:
            other = parsed[name]
            if other.get("clock") != first.get("clock"):
                all_errors.append(f"{name}: clock differs from {first_name}")
            if other.get("quantities") != first.get("quantities"):
                all_errors.append(f"{name}: quantities differ from {first_name}")
            if sorted(op["id"] for op in other.get("ops") or []) != first_ids:
                all_errors.append(f"{name}: operation set differs from {first_name}")

    result = {"ok": not all_errors, "errors": all_errors}
    print(json.dumps(result, indent=2))
    return 0 if not all_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
