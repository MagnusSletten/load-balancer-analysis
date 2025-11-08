import os, time, requests, statistics as stats, random, threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- targets & env ----
TARGETS = [
    ("LC", os.getenv("TARGET_LC", "http://nginx:8083/calculate")),
    ("RR", os.getenv("TARGET_RR", "http://nginx:8082/calculate")),
]
DURATION     = int(os.getenv("DURATION_SEC", "18"))
MODE         = os.getenv("JOB_MODE", "cycle")
JOBS_STR     = os.getenv("JOBS", "A,B,C,D,E")
WEIGHTS_STR  = os.getenv("JOB_WEIGHTS", "")
REQ_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT_SEC", "120"))

# NEW: steady-stream controls
CONCURRENCY  = int(os.getenv("CONCURRENCY", "30"))   # number of in-flight requests to maintain
TARGET_RPS   = float(os.getenv("TARGET_RPS", "100"))     # 0 = unlimited launch rate; else open-loop pacing (req/s)

# ---- job config ----
JOBS = [j.strip().upper() for j in JOBS_STR.split(",") if j.strip()]
W = {}
if WEIGHTS_STR:
    for tok in WEIGHTS_STR.split(","):
        k, v = tok.split(":")
        W[k.strip().upper()] = float(v)

# ---- thread-local session ----
_tls = threading.local()
def get_session():
    if not hasattr(_tls, "s"):
        _tls.s = requests.Session()
    return _tls.s

def one_call(url, job, deadline, connect_timeout=2.0, floor=0.25):
    s = get_session()
    t0 = time.perf_counter()
    # time left until the deadline (plus a tiny floor so timeout isn't 0)
    remaining = max(floor, deadline - t0)
    # requests allows (connect, read) tuple; clamp by REQUEST_TIMEOUT too
    read_timeout = min(remaining, REQ_TIMEOUT)
    try:
        r = s.get(url, params={"job": job}, timeout=(connect_timeout, read_timeout))
        r.raise_for_status()
        elapsed = time.perf_counter() - t0
        try:
            jj = r.json().get("job") or job
        except Exception:
            jj = job
        return jj, elapsed, True
    except Exception:
        return job, time.perf_counter() - t0, False

def run_case(name, url):
    results = []
    per_job = defaultdict(list)
    fails = 0

    # deadline uses monotonic clock
    t_end = time.perf_counter() + DURATION

    # job picker
    if MODE == "weighted" and W:
        weights = list(W.items())
        keys, vals = zip(*weights)
        tot = sum(vals)
        probs = [v/tot for v in vals]
        def pick_job(_i):
            r = random.random(); acc = 0.0
            for k, p in zip(keys, probs):
                acc += p
                if r <= acc: return k
            return keys[-1]
    else:
        def pick_job(i):
            return JOBS[i % len(JOBS)]

    # open-loop pacing (optional)
    inter_arrival = (1.0 / TARGET_RPS) if TARGET_RPS > 0 else 0.0
    next_launch = time.perf_counter()
    launched = 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        in_flight = set()

        # seed pool -------------- FIX: pass t_end
        for _ in range(CONCURRENCY):
            job = pick_job(launched); launched += 1
            in_flight.add(pool.submit(one_call, url, job, t_end))

        # main loop: refill immediately on completion until deadline
        while time.perf_counter() < t_end:
            done, in_flight = _wait_one(in_flight)
            for f in done:
                job, lat, ok = f.result()
                if ok and time.perf_counter() <= t_end:
                    results.append(lat); per_job[job].append(lat)
                elif not ok:
                    fails += 1

                # refill ------------- FIX: pass t_end
                if time.perf_counter() < t_end:
                    if inter_arrival > 0:
                        now = time.perf_counter()
                        if now < next_launch:
                            time.sleep(next_launch - now)
                        next_launch = max(next_launch + inter_arrival, time.perf_counter())
                    job = pick_job(launched); launched += 1
                    in_flight.add(pool.submit(one_call, url, job, t_end))

        # drain without refilling; still count only <= deadline
        while in_flight:
            done, in_flight = _wait_one(in_flight)
            for f in done:
                job, lat, ok = f.result()
                if ok and time.perf_counter() <= t_end:
                    results.append(lat); per_job[job].append(lat)
                elif not ok:
                    fails += 1

    if not results:
        print(f"[{name}] no results (fails={fails})"); return

    results.sort()
    p50 = stats.median(results)
    p95 = results[int(0.95 * len(results)) - 1]
    p99 = results[int(0.99 * len(results)) - 1]
    rps = len(results) / DURATION
    print(f"[{name}] RPS={rps:.1f} p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s n={len(results)} "
          f"(concurrency={CONCURRENCY}, target_rps={TARGET_RPS or '-'}, mode={MODE}, fails={fails})")
    for job in sorted(per_job.keys()):
        arr = per_job[job]
        arr.sort()
        jp50 = stats.median(arr)
        jp95 = arr[int(0.95 * len(arr)) - 1]
        print(f"   - job {job}: n={len(arr)} p50={jp50:.3f}s p95={jp95:.3f}s")

def _wait_one(futures_set):
    done = set()
    for f in as_completed(futures_set, timeout=None):
        done.add(f)
        break
    return done, (futures_set - done)

COOLDOWN_SEC = float(os.getenv("COOLDOWN_SEC", "20"))

if __name__ == "__main__":
    print(f"DURATION={DURATION}s; jobs={JOBS}; mode={MODE}; "
          f"weights={(W if MODE=='weighted' else '-')}; "
          f"concurrency={CONCURRENCY}; target_rps={TARGET_RPS or '-'}; timeout={REQ_TIMEOUT}s")
    for i, (name, url) in enumerate(TARGETS):
        run_case(name, url)
        if i < len(TARGETS) - 1:
            print(f"[cooldown] sleeping {COOLDOWN_SEC}s to let backends drain…")
            time.sleep(COOLDOWN_SEC)