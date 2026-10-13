"""Turn the raw result files in docs/benchmark-results/ into Markdown tables.

Nothing is typed in by hand: every number printed here comes from a result file.
Usage:  uv run python scripts/benchmark/summarize.py > /tmp/tables.md
"""

import glob
import json
import statistics
from pathlib import Path
from typing import Any

RESULTS = Path(__file__).resolve().parents[2] / "docs" / "benchmark-results"


def load(scenario: str) -> list[dict[str, Any]]:
    out = []
    for path in sorted(glob.glob(str(RESULTS / f"*_{scenario}_*.json"))):
        data = json.loads(Path(path).read_text())
        data["_file"] = Path(path).name
        out.append(data)
    return out


def f(x: float | None, digits: int = 3) -> str:
    # Older result files hold exact-decimal values saved as text, so accept both.
    return "n/a" if x is None else f"{float(x):.{digits}f}"


def spread(values: list[float], digits: int = 1) -> str:
    """median (min-max) across repeated runs."""
    if not values:
        return "n/a"
    return f"{statistics.median(values):.{digits}f} ({min(values):.{digits}f}-{max(values):.{digits}f})"


def section(title: str) -> None:
    print(f"\n#### {title}\n")


def submit_only() -> None:
    rows = load("submit-only")
    if not rows:
        return
    section("API submit cost (no workers running)")
    print(
        "| Label | Concurrency | Jobs already queued | Accepted/s | Latency p50 (s) | p95 | p99 | Statuses |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        s, lat = r["submit"], r["submit"]["latency_seconds_accepted"]
        print(
            f"| {r['label']} | {r['parameters']['concurrency']} | {r['parameters'].get('deep_queue', 0)} "
            f"| {f(s['accepted_per_second'], 1)} "
            f"| {f(lat['p50'])} | {f(lat['p95'])} | {f(lat['p99'])} | {s['status_counts']} |"
        )

    def group(prefix: str) -> list[dict[str, Any]]:
        return [r for r in rows if r["label"].startswith(prefix) and "c16" in r["label"]]

    for name, prefix in (("limiter ON", "limiter-on"), ("limiter OFF", "limiter-off")):
        g = [r for r in group(prefix) if "history" not in r["label"]]
        if g:
            rates = [r["submit"]["accepted_per_second"] for r in g]
            p50s = [r["submit"]["latency_seconds_accepted"]["p50"] for r in g]
            print(
                f"\nConcurrency 16, {name}: {len(g)} runs, accepted/s median (min-max) = "
                f"{spread(rates)}; p50 latency median (min-max) = {spread(p50s, 3)} s"
            )


def setup(p: dict[str, Any]) -> str:
    parts = []
    parts.append("1M history" if p.get("keep_history") else "empty database")
    if p.get("vacuum_first"):
        parts.append("vacuumed first")
    if p.get("without_new_indexes"):
        parts.append("new indexes dropped")
    return ", ".join(parts)


def drain() -> None:
    rows = load("drain")
    if not rows:
        return
    section("Worker drain capacity (queue created in the database, workers empty it)")
    print(
        "| Label | Workers | Job kind | Setup | Runs | Jobs/s median (min-max) | Per-worker share spread "
        "| Errors / extra attempts | CPU% of one core: db / worker (mean per container) |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        runs = r["runs"]
        rates = [x["drain_jobs_per_second"] for x in runs]
        shares = [w for x in runs for w in x["jobs_per_worker"]]
        spread_share = (
            f"{min(shares)}-{max(shares)} jobs/worker" if len(shares) > 1 else "single worker"
        )
        errs = sum(x["jobs"]["failed"] + x["jobs"]["dead_lettered"] for x in runs)
        extra = sum(x["jobs"]["extra_attempts"] for x in runs)
        cont = runs[-1].get("containers", {})
        db = cont.get("db", {}).get("cpu_percent_mean_per_container")
        wk = cont.get("worker", {}).get("cpu_percent_mean_per_container")
        p = r["parameters"]
        print(
            f"| {r['label']} | {p['workers']} | {p['kind']} | {setup(p)} | {len(runs)} | {spread(rates)} "
            f"| {spread_share} | {errs} / {extra} | {f(db, 0)} / {f(wk, 0)} |"
        )


def idle() -> None:
    for r in load("idle-latency"):
        section("Idle pickup latency (one job at a time into an idle system)")
        j = r["runs"][0]["jobs"]
        w = j["queue_wait_seconds"]
        print(
            f"{j['jobs']} jobs, {r['parameters']['workers']} idle workers (poll interval 1 s). "
            f"Queue wait p50 / p95 / p99 / max = {f(w['p50'])} / {f(w['p95'])} / {f(w['p99'])} / "
            f"{f(w['max'])} s. End-to-end p50 = {f(j['end_to_end_seconds']['p50'])} s."
        )


def fairness() -> None:
    for r in load("fairness"):
        section("Fairness: client B's wait with and without client A's backlog")
        print("| Case | Jobs queued when B started | B queue wait p50 (s) | p95 | max |")
        print("|---|---|---|---|---|")
        for case, res in r["results"].items():
            w = res["b"]["queue_wait_seconds"]
            print(
                f"| {case} | {res['queued_jobs_when_b_started']} | {f(w['p50'])} | {f(w['p95'])} "
                f"| {f(w['max'])} |"
            )


def overload() -> None:
    for r in load("overload"):
        section("Overload and recovery")
        p = r["parameters"]
        print(
            f"{p['clients']} clients x {p['rate_per_client']}/s for {p['seconds']} s against "
            f"{p['workers']} workers; queue caps {p['per_client_cap']} per client / "
            f"{p['total_cap']} total.\n"
        )
        print(f"- HTTP statuses returned: {r['status_counts']}")
        for status, lat in r["latency_by_status"].items():
            print(
                f"  - status {status}: n={lat['n']}, latency p50/p95/p99 = {f(lat['p50'])} / "
                f"{f(lat['p95'])} / {f(lat['p99'])} s"
            )
        j = r["accepted_jobs"]
        print(
            f"- Accepted jobs: {j['jobs']}; succeeded {j['succeeded']}; failed/dead-lettered "
            f"{j['failed'] + j['dead_lettered']}; unfinished {j['unfinished']}; extra attempts "
            f"{j['extra_attempts']}"
        )
        w = j["queue_wait_seconds"]
        print(
            f"- Accepted jobs' queue wait p50/p95/p99 = {f(w['p50'])} / {f(w['p95'])} / {f(w['p99'])} s"
        )
        print(
            f"- Drained after load stopped: {r['drained']} in "
            f"{f(r['drain_seconds_after_load_stopped'], 1)} s; submit after recovery: "
            f"HTTP {r['submit_after_recovery_status']}"
        )
        cont = r.get("containers", {})
        print("- Peak CPU% (of one core) per container: " + ", ".join(
            f"{k} {v['cpu_percent_max_per_container']:.0f}" for k, v in cont.items()))  # fmt: skip


def steady() -> None:
    for r in load("steady"):
        section("Steady state at fixed arrival rates")
        print(
            "| Arrival rate (jobs/s) | Jobs | Completed/s | Queue wait p50 (s) | p95 | p99 "
            "| End-to-end p95 (s) | Errors |"
        )
        print("|---|---|---|---|---|---|---|---|")
        for run in r["runs"]:
            j = run["jobs"]
            w, e = j["queue_wait_seconds"], j["end_to_end_seconds"]
            print(
                f"| {run['arrival_rate_per_second']} | {j['jobs']} | {f(j['completion_per_second'], 1)} "
                f"| {f(w['p50'])} | {f(w['p95'])} | {f(w['p99'])} | {f(e['p95'])} "
                f"| {j['failed'] + j['dead_lettered']} |"
            )


def soak() -> None:
    for r in load("soak"):
        section("Soak")
        j = r["run"]["jobs"]
        print(
            f"{r['parameters']['seconds']:.0f} s at {r['parameters']['rate']}/s with "
            f"{r['parameters']['workers']} workers: {headline(j)}\n"
        )
        series = r["time_series"]
        print("| Elapsed (s) | Completed in window | Queue wait p95 in window (s) | Queued now | jobs table dead rows | jobs table size (MB) |")  # fmt: skip
        print("|---|---|---|---|---|---|")
        for s in series[:: max(1, len(series) // 10)]:
            t = s["tables"].get("jobs", {})
            print(
                f"| {s['elapsed_seconds']} | {s['completed_in_window']} "
                f"| {f(s['queue_wait_p95_seconds_in_window'])} | {s['queued_now']} "
                f"| {t.get('dead_rows', 'n/a')} | {t.get('total_bytes', 0) / 1e6:.1f} |"
            )
        cont = r["run"].get("containers", {})
        print("\nContainer peaks: " + "; ".join(
            f"{k}: CPU {v['cpu_percent_max_per_container']:.0f}% of a core, "
            f"memory {v['memory_mib_max_per_container']:.0f} MiB" for k, v in cont.items()))  # fmt: skip


def headline(j: dict[str, Any]) -> str:
    return (
        f"{j['jobs']} jobs, {j['succeeded']} succeeded, {j['failed'] + j['dead_lettered']} failed, "
        f"{j['unfinished']} unfinished, {j['extra_attempts']} extra attempts; queue wait p50/p95/p99 = "
        f"{f(j['queue_wait_seconds']['p50'])}/{f(j['queue_wait_seconds']['p95'])}/"
        f"{f(j['queue_wait_seconds']['p99'])} s"
    )


def plan_ms(result: dict[str, Any], section: str = "current_schema") -> dict[str, float]:
    return {
        name: statistics.median(q["execution_ms_each_run"])
        for name, q in result[section].items()
        if q["execution_ms_each_run"]
    }


def history() -> None:
    for r in load("preload"):
        section("Preloaded history")
        print(f"Tables after preload: {r['tables']} (took {r['seconds']:.0f} s)")
    plans = {r["label"]: r for r in load("plans")}
    before = plans.get("1m")
    after = plans.get("1m-after-indexes")
    if before and after:
        section(
            "Query timings with 1,000,000 finished jobs and 20,000 queued: before and after the two partial indexes"
        )
        print("Median of 3 runs of EXPLAIN (ANALYZE), milliseconds.\n")
        print("| Query | Before (ms) | After (ms) | Before / After |")
        print("|---|---|---|---|")
        b, a = plan_ms(before), plan_ms(after)
        for name in b:
            ratio = b[name] / a[name] if a.get(name) else float("nan")
            print(f"| {name} | {b[name]:.2f} | {a.get(name, float('nan')):.2f} | {ratio:.2f}x |")
    deep = plans.get("1m-queue100k")
    if deep:
        section("Cost of the queue-cap checks as the queue gets deeper (before any cache)")
        shallow_ms = plan_ms(before) if before else {}
        deep_ms = plan_ms(deep)
        print("| Query | 20,000 queued | 100,000 queued |")
        print("|---|---|---|")
        for name in ("per_client_queue_count", "global_queue_count", "claim_next_job"):
            print(
                f"| {name} | {shallow_ms.get(name, float('nan')):.2f} ms | {deep_ms[name]:.2f} ms |"
            )
    scrapes = load("scrape")
    if scrapes:
        section("GET /metrics scrape time (seconds), repeated back to back")
        print("| Label | Scrapes in order |")
        print("|---|---|")
        for r in scrapes:
            print(f"| {r['label']} | {[round(x, 3) for x in r['seconds']]} |")
    for label, r in plans.items():
        if r.get("metrics_scrape_seconds"):
            scrape_times = [round(x, 2) for x in r["metrics_scrape_seconds"]]
            print(f"\nplans run '{label}': GET /metrics seconds {scrape_times}")


def main() -> None:
    for fn in (submit_only, drain, idle, fairness, overload, steady, soak, history):
        fn()


if __name__ == "__main__":
    main()
