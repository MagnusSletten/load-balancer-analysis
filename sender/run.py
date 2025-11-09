#!/usr/bin/env python3
import os, time, requests, statistics as stats, random, threading
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────────────────────────
# Environment / defaults
# ──────────────────────────────────────────────────────────────────────────────
TARGETS = [
    ("LC", os.getenv("TARGET_LC", "http://nginx:8083/calculate")),
    ("RR", os.getenv("TARGET_RR", "http://nginx:8082/calculate")),
]

DURATION       = int(os.getenv("DURATION_SEC", "18"))      # window for steady-stream mode
MODE           = os.getenv("JOB_MODE", "cycle")            # "cycle" | "weighted"
JOBS_STR       = os.getenv("JOBS", "A,B,C,D,E")
WEIGHTS_STR    = os.getenv("JOB_WEIGHTS", "")              # e.g. "A:6,C:3,E:1"
REQ_TIMEOUT    = float(os.getenv("REQUEST_TIMEOUT_SEC", "120"))

CONCURRENCY    = int(os.getenv("CONCURRENCY", "12"))       # steady-stream: in-flight requests target
TARGET_RPS     = float(os.getenv("TARGET_RPS", "0"))       # 0 = unlimited; else open-loop pacing (req/s)

START_DELAY    = float(os.getenv("START_DELAY_SEC", "6"))  # delay before first run
COOLDOWN_SEC   = float(os.getenv("COOLDOWN_SEC", "20"))    # wait between LC/RR runs

RUN_STREAM     = int(os.getenv("RUN_STREAM", "1"))         # 1 to run steady-stream test
RUN_BATCH      = int(os.getenv("RUN_BATCH", "0"))          # 1 to run batched bursts test after stream
WARMUP_K       = int(os.getenv("WARMUP_BATCH_K", "0"))     # small warmup burst (per target) before tests

# Batched-burst mode knobs
BATCH_COUNT    = int(os.getenv("BATCH_COUNT", "0"))        # 0 disables; else run N batches per target
BATCH_MIN      = int(os.getenv("BATCH_MIN", "10"))
BATCH_MAX      = int(os.getenv("BATCH_MAX", "15"))
BATCH_REQUESTS = int(os.getenv("BATCH_REQUESTS", "0"))     # 0 = use legacy range

CAPTURE_UPSTREAM = int(os.getenv("CAPTURE_UPSTREAM", "0")) # set to 1 if nginx adds X-Upstream header

# ──────────────────────────────────────────────────────────────────────────────
# Jobs & weights
# ──────────────────────────────────────────────────────────────────────────────
JOBS = [j.strip().upper() for j in JOBS_STR.split(",") if j.strip()]
W = {}
if WEIGHTS_STR:
    for tok in WEIGHTS_STR.split(","):
        k, v = tok.split(":")
        W[k.strip().upper()] = float(v)

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
_tls = threading.local()
def get_session():
    if not hasattr(_tls, "s"):
        _tls.s = requests.Session()
    return _tls.s

def pct(arr, q: float):
    """Quantile helper on sorted arr, safe for small n."""
    if not arr:
        return 0.0
    i = max(0, min(len(arr)-1, int(round(q*(len(arr)-1)))))
    return arr[i]

def make_picker():
    """Return a zero-arg function that picks the next job according to MODE/W."""
    if MODE == "weighted" and W:
        keys, vals = zip(*W.items())
        tot = sum(vals); probs = [v/tot for v in vals]
        def pick():
            r = random.random(); acc = 0.0
            for k, p in zip(keys, probs):
                acc += p
                if r <= acc:
                    return k
            return keys[-1]
        return pick
    else:
        # deterministic cycle across JOBS
        i = 0
        def pick():
            nonlocal i
            j = JOBS[i % len(JOBS)]
            i += 1
            return j
        return pick

def one_call(url, job, deadline, connect_timeout=2.0, floor=0.25):
    """
    Fire one request and return (job, latency, ok[, upstream]).
    Deadline is a perf_counter timestamp; we clamp read timeout to it.
    """
    s = get_session()
    t0 = time.perf_counter()
    remaining = max(floor, deadline - t0)
    read_timeout = min(remaining, REQ_TIMEOUT)
    try:
        r = s.get(url, params={"job": job}, timeout=(connect_timeout, read_timeout))
        r.raise_for_status()
        elapsed = time.perf_counter() - t0
        try:
            jj = r.json().get("job") or job
        except Exception:
            jj = job
        if CAPTURE_UPSTREAM:
            return jj, elapsed, True, r.headers.get("X-Upstream")
        else:
            return jj, elapsed, True
    except Exception:
        if CAPTURE_UPSTREAM:
            return job, time.perf_counter() - t0, False, None
        else:
            return job, time.perf_counter() - t0, False

def _wait_one(futures_set):
    """Wait for exactly one future to complete; return (done_set, remaining_set)."""
    done = set()
    for f in as_completed(futures_set, timeout=None):
        done.add(f)
        break
    return done, (futures_set - done)

def _print_stream_summary(name, results, per_job, fails, label):
    if not results:
        print(f"[{name}] no results (fails={fails})")
        return
    results.sort()
    p50, p95, p99 = pct(results, 0.5), pct(results, 0.95), pct(results, 0.99)
    rps = len(results) / DURATION
    print(f"[{name}] RPS={rps:.1f} p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s n={len(results)} "
          f"(concurrency={CONCURRENCY}, target_rps={TARGET_RPS or '-'}, mode={MODE}, fails={fails})")
    for job in sorted(per_job.keys()):
        arr = sorted(per_job[job])
        jp50, jp95 = pct(arr, 0.5), pct(arr, 0.95)
        print(f"   - job {job}: n={len(arr)} p50={jp50:.3f}s p95={jp95:.3f}s")
    if label:
        print(label)

# ──────────────────────────────────────────────────────────────────────────────
# Steady-stream runner (closed-loop with optional open-loop pacing)
# ──────────────────────────────────────────────────────────────────────────────
def run_case(name, url):
    pick_job = make_picker()
    results = []
    per_job = defaultdict(list)
    per_upstream = Counter() if CAPTURE_UPSTREAM else None
    fails = 0

    t_end = time.perf_counter() + DURATION
    inter_arrival = (1.0 / TARGET_RPS) if TARGET_RPS > 0 else 0.0
    next_launch = time.perf_counter()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        in_flight = set()

        # Seed pool
        for _ in range(CONCURRENCY):
            job = pick_job()
            if CAPTURE_UPSTREAM:
                in_flight.add(pool.submit(one_call, url, job, t_end))
            else:
                in_flight.add(pool.submit(one_call, url, job, t_end))

        # Main loop
        while time.perf_counter() < t_end:
            done, in_flight = _wait_one(in_flight)
            for f in done:
                if CAPTURE_UPSTREAM:
                    job, lat, ok, upstream = f.result()
                else:
                    job, lat, ok = f.result()
                    upstream = None

                if ok and time.perf_counter() <= t_end:
                    results.append(lat); per_job[job].append(lat)
                    if per_upstream is not None and upstream:
                        per_upstream[(job, upstream)] += 1
                elif not ok:
                    fails += 1

                # Refill
                if time.perf_counter() < t_end:
                    if inter_arrival > 0:
                        now = time.perf_counter()
                        if now < next_launch:
                            time.sleep(next_launch - now)
                        next_launch = max(next_launch + inter_arrival, time.perf_counter())
                    job = pick_job()
                    if CAPTURE_UPSTREAM:
                        in_flight.add(pool.submit(one_call, url, job, t_end))
                    else:
                        in_flight.add(pool.submit(one_call, url, job, t_end))

        # Drain (don’t refill)
        while in_flight:
            done, in_flight = _wait_one(in_flight)
            for f in done:
                if CAPTURE_UPSTREAM:
                    job, lat, ok, upstream = f.result()
                else:
                    job, lat, ok = f.result()
                    upstream = None

                if ok and time.perf_counter() <= t_end:
                    results.append(lat); per_job[job].append(lat)
                    if per_upstream is not None and upstream:
                        per_upstream[(job, upstream)] += 1
                elif not ok:
                    fails += 1

    label = None
    if per_upstream:
        label = "   - upstream matrix (job×upstream completes): " + ", ".join(
            [f"{j}@{u}={c}" for (j,u), c in per_upstream.most_common()]
        )
    _print_stream_summary(name, results, per_job, fails, label)

# ──────────────────────────────────────────────────────────────────────────────
# Batched-burst runner (open-loop bursts, measure per-batch wall time)
# ──────────────────────────────────────────────────────────────────────────────
def run_batched_case(name, url, batches, batch_requests=50, concurrency=10):
    assert batches > 0 and batch_requests >= 1 and concurrency >= 1
    pick_job = make_picker()

    batch_times, per_batch_ok, per_batch_fail = [], [], []
    total_ok = total_fail = 0

    for _ in range(batches):
        t0 = time.perf_counter()
        ok = fail = 0
        deadline = time.perf_counter() + REQ_TIMEOUT

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futs = [pool.submit(one_call, url, pick_job(), deadline)
                    for _ in range(batch_requests)]
            for f in as_completed(futs):
                _, _, success = f.result()
                if success:
                    ok += 1
                else:
                    fail += 1

        elapsed = time.perf_counter() - t0
        batch_times.append(elapsed)
        per_batch_ok.append(ok); per_batch_fail.append(fail)

        total_ok += ok; total_fail += fail

    sorted_bt = sorted(batch_times)
    p50 = pct(sorted_bt, 0.5)
    p95 = pct(sorted_bt, 0.95)
    worst = sorted_bt[-1] if sorted_bt else 0.0
    total_sum = sum(batch_times)
    total_requests = batches * batch_requests
    eff_rps = (total_requests / total_sum) if total_sum > 0 else 0.0

    print(f"[{name} BATCH] batches={batches} batch_requests={batch_requests} concurrency={concurrency} "
          f"sum={total_sum:.3f}s p50={p50:.3f}s p95={p95:.3f}s worst={worst:.3f}s "
          f"reqs={total_requests} ok={total_ok} fail={total_fail} effRPS={eff_rps:.2f}")
    for idx, (bt, ok, fail) in enumerate(zip(batch_times, per_batch_ok, per_batch_fail), start=1):
        print(f"   - batch {idx:02d}: K={batch_requests} time={bt:.3f}s ok={ok} fail={fail}")

# ──────────────────────────────────────────────────────────────────────────────
# Warmup (small burst to establish keepalives)
# ──────────────────────────────────────────────────────────────────────────────
def warmup(url):
    if WARMUP_K <= 0:
        return
    deadline = time.perf_counter() + 5.0
    with ThreadPoolExecutor(max_workers=WARMUP_K) as pool:
        futs = [pool.submit(one_call, url, JOBS[0], deadline) for _ in range(WARMUP_K)]
        for _ in as_completed(futs):
            pass

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"DURATION={DURATION}s; jobs={JOBS}; mode={MODE}; "
          f"weights={(W if MODE=='weighted' else '-')}; "
          f"concurrency={CONCURRENCY}; target_rps={TARGET_RPS or '-'}; timeout={REQ_TIMEOUT}s")
    if RUN_BATCH and BATCH_COUNT > 0:
        if BATCH_REQUESTS > 0:
            print(f"BATCH_COUNT={BATCH_COUNT} batch_requests={BATCH_REQUESTS} concurrency={CONCURRENCY}")
        else:
            print(f"BATCH_COUNT={BATCH_COUNT} size_range=[{BATCH_MIN},{BATCH_MAX}]")
    if START_DELAY > 0:
        print(f"[startup] waiting {START_DELAY:.1f}s before first run…")
        time.sleep(START_DELAY)

    for i, (name, url) in enumerate(TARGETS):
        if WARMUP_K:
            warmup(url)

        if RUN_STREAM:
            run_case(name, url)

        time.sleep(3*START_DELAY)

        if RUN_BATCH and BATCH_COUNT > 0:
            if BATCH_REQUESTS > 0:
                # New: fixed total requests per batch (decoupled from concurrency)
                run_batched_case(name, url, BATCH_COUNT, batch_requests=BATCH_REQUESTS, concurrency=CONCURRENCY)
            else:
                # Legacy behavior: random K per batch in [BATCH_MIN, BATCH_MAX]
                for _ in range(BATCH_COUNT):
                    K = random.randint(BATCH_MIN, BATCH_MAX)
                    run_batched_case(name, url, 1, batch_requests=K, concurrency=CONCURRENCY)

        if i < len(TARGETS) - 1:
            print(f"[cooldown] sleeping {COOLDOWN_SEC}s to let backends drain…")
            time.sleep(COOLDOWN_SEC)

