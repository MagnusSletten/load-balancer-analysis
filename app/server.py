from flask import Flask, request, jsonify
import os, hashlib, time

app = Flask(__name__)

# Define per-job iterations (tune to your box)
JOB_ITERS = {
    "A": 130_000_000,
    "B": 40_000_000,
    "C": 20_000_000,
    "D": 10_000_000,
    "E": 5_000_000,
    "F": 2_500_000,
    "G": 1_250_000,
    "H": 1_160_000,
    "I":   930_000,
    "J":   1_500_000,
    "K":   595_000,
    "L":   476_000,
    "M":   520_000,
    "N":   305_000,
    "O":   244_000,
    "P":   195_000,
    "Q":   156_000,
    "R":   125_000,
    "S":   100_000,
    "T":    80_000,
}

JOB_FACTOR = float(os.getenv("JOB_FACTOR", "1"))
JOB_ITERS = {k: max(1, int(v * JOB_FACTOR)) for k, v in JOB_ITERS.items()}

def do_work(iters: int):
    secret = b"pollapp"
    salt   = b"nginx-lb-demo"
    hashlib.pbkdf2_hmac("sha256", secret, salt, iters)

@app.get("/calculate")
def calculate():
    job = request.args.get("job", "A").upper()
    iters = JOB_ITERS.get(job)
    t0 = time.perf_counter()
    do_work(iters)
    elapsed = time.perf_counter() - t0
    return jsonify({"ok": True, "job": job, "iters": iters, "elapsed_sec": round(elapsed, 4)})