# Minimal test setup to benchmark load-balancers

## Introduction

Most load-balancer benchmarks measure simple round-robin throughput on uniform workloads.  
This setup is intentionally designed to expose how different algorithms behave under **heterogeneous CPU-bound jobs**, where request costs vary greatly.

Key characteristics:

- **Heterogeneous workloads**  
  Each request type (A, B, C, …) performs a different amount of CPU work, revealing how load balancers react to uneven job sizes.

- **Strategy-level evaluation**  
  The test isolates specific LB strategies:
  - Nginx: random, least-connections  
  - Caddy: random, least-connections  
  - Traefik: round-robin, **least-time** (recently introduced)
  - More strategies can also be added in a trivial way.
  


- **Per-upstream tracking**  
  Each backend tags itself with its container name (`upstream: app_X`), allowing the benchmark to capture:
  - how traffic is *actually* distributed  
  - whether a strategy starves or overfeeds certain upstreams  
  - how quickly a balancer rebalances after unfair routing

- **Deterministic or weighted job selection**  
  Useful for testing both consistent cycles and probability-based load patterns.

This makes it easy to compare fairness, stability, and performance across reverse proxies.

## To start

Running`docker-compose up` will build/pull containers and start them.

Un-comment lines in `docker-compose.yml` to test different reverse-proxies/strategies. 

### Environment Variable Overview

| Variable | Meaning |
|---------|---------|
| `DURATION_SEC=300` | Total duration (in seconds) for the steady-stream test mode. |
| `CAPTURE_UPSTREAM=1` | Include the upstream container name in responses so routing distribution can be measured. |
| `JOB_MODE=cycle` | Job selection strategy: cycle through jobs deterministically (A → B → C → A → …). |
| `JOBS=A,B,C` | List of job types; each job has a different compute cost. |
| `JOB_WEIGHTS=C:6,E:3,A:1` | Used only if `JOB_MODE=weighted`; defines probability weights. (Ignored when in `cycle` mode.) |
| `REQUEST_TIMEOUT_SEC=120` | Maximum allowed time per HTTP request. |
| `CONCURRENCY=5` | Number of simultaneous in-flight requests during tests. |
| `RUN_STREAM=0` | Enable (`1`) or disable (`0`) the steady-stream load test. |
| `RUN_BATCH=1` | Enable batched-burst load testing. |
| `BATCH_COUNT=5` | Number of batches to execute for the burst test. |
| `BATCH_REQUESTS=50` | Number of parallel requests per batch. |
| `START_DELAY_SEC=12` | Delay before the first test run, to allow containers to boot and stabilize. |
| `COOLDOWN_SEC=15` | Cooldown time between different target tests, letting backends drain previous load. |
