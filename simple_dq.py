"""Simple data-quality monitor (needs pandas and numpy: pip install -r requirements.txt).

Flow:  CSV batch -> validate each row -> bad rows go to `rejects`
       -> good rows go to `transactions` -> profile the batch
       -> compare with the baseline profile -> write explainable `alerts`.

Commands:
  python simple_dq.py demo
  python simple_dq.py bench --rows 100000
  python simple_dq.py generate --out data/b1.csv --day 2026-10-01 --scenario normal
  python simple_dq.py run --file data/b1.csv --customers data/customers.csv --as-of 2026-10-02T01:00:00Z --baseline
"""
import argparse
import csv
import hashlib
import json
import logging
import random
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------- settings
REQUIRED = ["txn_id", "customer_id", "amount", "status", "event_ts"]
VALID_STATUS = {"completed", "pending", "failed", "refunded"}
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"          # ISO-8601, UTC

# alert thresholds (change these to make alerts stricter or looser)
MIN_VOLUME_RATIO = 0.5      # batch has < 50% (or > 200%) of baseline rows
MAX_NULL_INCREASE = 0.02    # null rate up by more than 2 percentage points
MAX_MEAN_SHIFT_STD = 0.5    # amount mean moved by more than 0.5 baseline std
MAX_SHARE_CHANGE = 0.10     # a status share changed by more than 10 points
MAX_LAG_HOURS = 24          # newest event older than 24h when loaded
MAX_REJECT_RATE = 0.05      # more than 5% of rows rejected


log = logging.getLogger("dq")


# ---------------------------------------------------------------- small helpers
def parse_ts(text):
    return datetime.strptime(text, TS_FORMAT).replace(tzinfo=timezone.utc)


def file_hash(path):
    """batch_id: same file contents always give the same id (used to detect reruns)."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]


# ---------------------------------------------------------------- 1. read the CSV into a DataFrame
def read_batch(path):
    """Return (header, DataFrame of text, malformed_rows). Every row keeps its line number."""
    rows, line_numbers, malformed = [], [], []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        for values in reader:
            if not values:
                continue
            if len(values) != len(header):          # wrong number of fields -> malformed
                malformed.append((reader.line_num, ",".join(values)))
            else:
                rows.append(values)
                line_numbers.append(reader.line_num)
    df = pd.DataFrame(rows, columns=header or REQUIRED, dtype=str)
    for col in df.columns:                          # strip spaces; empty text becomes NaN (null)
        text = df[col].str.strip()
        df[col] = text.mask(text == "")
    df["line_no"] = line_numbers
    return header, df, malformed


# ---------------------------------------------------------------- 2. validate with DataFrame checks
def check_rows(df, customers):
    """Return a Series with a reject reason for every row ('' means the row is valid)."""
    reason = pd.Series("", index=df.index, dtype=object)

    def mark(mask, text):                           # first failing rule wins
        reason[mask & (reason == "")] = text

    for col in REQUIRED:                            # null checks
        mark(df[col].isna(), f"missing_{col}")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    mark(df["amount"].notna() & (amount.isna() | np.isinf(amount)), "invalid_amount")
    mark(amount < 0, "negative_amount")
    mark(df["status"].notna() & ~df["status"].isin(VALID_STATUS), "invalid_status")
    stamp = pd.to_datetime(df["event_ts"], format=TS_FORMAT, errors="coerce", utc=True)
    mark(df["event_ts"].notna() & stamp.isna(), "invalid_event_ts")
    mark(df["customer_id"].notna() & ~df["customer_id"].isin(customers), "unknown_customer")
    still_ok = reason == ""                         # duplicates: keep the first valid one
    repeated = df.loc[still_ok, "txn_id"].duplicated()
    reason[repeated[repeated].index] = "duplicate_txn_id"
    return reason


# ---------------------------------------------------------------- 2b. profile
def make_profile(df, row_count, as_of):
    """Summarise a batch: volume, null rates, amount stats, status mix, freshness."""
    amount = pd.to_numeric(df["amount"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    stamp = pd.to_datetime(df["event_ts"], format=TS_FORMAT, errors="coerce", utc=True)
    nulls = df[REQUIRED].isna().mean() if len(df) else pd.Series(0.0, index=REQUIRED)
    profile = {
        "row_count": row_count,
        "null_rate": {c: float(v) for c, v in nulls.items()},
        "amount_mean": float(amount.mean()) if amount.count() else None,
        "amount_std": float(amount.std(ddof=0)) if amount.count() > 1 else None,
        "status_share": {k: float(v) for k, v in df["status"].value_counts(normalize=True).items()},
    }
    if stamp.notna().any():
        newest = stamp.max()
        profile["max_event_ts"] = newest.strftime(TS_FORMAT)
        profile["lag_hours"] = (as_of - newest).total_seconds() / 3600
    else:
        profile["max_event_ts"], profile["lag_hours"] = None, None
    return profile


# ---------------------------------------------------------------- 3. compare -> alerts
def compare(base, cur, rows_rejected):
    """Return a list of alerts. Each one explains what changed and by how much."""
    alerts = []

    def alert(rule, column, message):
        alerts.append({"rule": rule, "column": column, "message": message})

    if cur["row_count"] == 0:
        alert("empty_batch", "*", f"Batch has 0 rows; baseline had {base['row_count']}.")
        return alerts

    ratio = cur["row_count"] / base["row_count"]
    if ratio < MIN_VOLUME_RATIO or ratio > 1 / MIN_VOLUME_RATIO:
        alert("volume_change", "*", f"Row count {cur['row_count']} is {ratio:.2f}x the baseline "
                                    f"({base['row_count']}).")

    reject_rate = rows_rejected / cur["row_count"]
    if reject_rate > MAX_REJECT_RATE:
        alert("reject_rate", "*", f"{reject_rate:.1%} of rows were rejected (limit {MAX_REJECT_RATE:.0%}). "
                                  "See the rejects table.")

    for col in REQUIRED:
        increase = cur["null_rate"][col] - base["null_rate"][col]
        if increase > MAX_NULL_INCREASE:
            alert("null_rate_increase", col, f"{col}: null rate rose from {base['null_rate'][col]:.1%} "
                                             f"to {cur['null_rate'][col]:.1%}.")

    if base["amount_std"] and cur["amount_mean"] is not None:
        shift = (cur["amount_mean"] - base["amount_mean"]) / base["amount_std"]
        if abs(shift) > MAX_MEAN_SHIFT_STD:
            alert("amount_mean_shift", "amount", f"amount: mean moved from {base['amount_mean']:.2f} to "
                                                 f"{cur['amount_mean']:.2f} ({shift:+.2f} baseline std).")

    for status in sorted(set(base["status_share"]) | set(cur["status_share"])):
        before = base["status_share"].get(status, 0.0)
        after = cur["status_share"].get(status, 0.0)
        if abs(after - before) > MAX_SHARE_CHANGE:
            alert("status_mix_change", "status", f"status '{status}' went from {before:.1%} to {after:.1%} "
                                                 "of rows.")

    if cur["lag_hours"] is None:
        alert("freshness_unknown", "event_ts", "No valid event_ts in the batch.")
    elif cur["lag_hours"] > MAX_LAG_HOURS:
        alert("stale_data", "event_ts", f"Newest event is {cur['lag_hours']:.1f}h old "
                                         f"(limit {MAX_LAG_HOURS}h); newest = {cur['max_event_ts']}.")
    return alerts


# ---------------------------------------------------------------- 4. database
SCHEMA = """
CREATE TABLE IF NOT EXISTS customers(customer_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS transactions(
    txn_id TEXT PRIMARY KEY, customer_id TEXT, amount REAL, status TEXT,
    event_ts TEXT, run_id TEXT, source_file TEXT);
CREATE TABLE IF NOT EXISTS rejects(
    run_id TEXT, source_file TEXT, line_no INTEGER, reason TEXT, raw_record TEXT);
CREATE TABLE IF NOT EXISTS baseline(id INTEGER PRIMARY KEY AUTOINCREMENT, profile_json TEXT);
CREATE TABLE IF NOT EXISTS alerts(run_id TEXT, rule TEXT, column_name TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS runs(
    run_id TEXT PRIMARY KEY, batch_id TEXT, source_file TEXT, status TEXT,
    started_at TEXT, ended_at TEXT, rows_in INTEGER, rows_valid INTEGER,
    rows_rejected INTEGER, alerts INTEGER, failure_reason TEXT);
"""


def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def now_iso():
    return datetime.now(timezone.utc).strftime(TS_FORMAT)


# ---------------------------------------------------------------- 5. the pipeline
def run_batch(path, db_path, customers_path=None, as_of=None, is_baseline=False):
    """Load one batch. Safe to rerun: the same file is skipped. Returns a result dict."""
    as_of = parse_ts(as_of) if as_of else datetime.now(timezone.utc)
    batch_id = file_hash(path)
    conn = connect(db_path)
    if customers_path:
        with open(customers_path, newline="") as f, conn:
            conn.executemany("INSERT OR IGNORE INTO customers VALUES (?)",
                             [(r["customer_id"],) for r in csv.DictReader(f)])

    already = conn.execute("SELECT run_id FROM runs WHERE batch_id=? AND status='SUCCESS'",
                           (batch_id,)).fetchone()
    result = {"status": "SKIPPED", "rows_in": 0, "rows_valid": 0, "rows_rejected": 0, "alerts": []}
    if already:
        result["note"] = f"already loaded by {already[0]}"
        log.info("skipped batch_id=%s file=%s (%s)", batch_id, path, result["note"])
        return result

    run_id = f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}_{batch_id[:6]}"
    started = now_iso()
    conn.execute("INSERT INTO runs(run_id,batch_id,source_file,status,started_at) VALUES (?,?,?,?,?)",
                 (run_id, batch_id, str(path), "RUNNING", started))
    conn.commit()
    log.info("[%s] start file=%s batch_id=%s baseline=%s", run_id, path, batch_id, is_baseline)

    try:
        customers = {r[0] for r in conn.execute("SELECT customer_id FROM customers")}
        header, df, malformed = read_batch(path)
        missing = [c for c in REQUIRED if header and c not in header]
        if missing:
            raise ValueError(f"required column(s) missing: {missing}")

        reason = check_rows(df, customers)
        bad = reason != ""
        good = df[~bad]                              # valid rows stay in a DataFrame
        rejects = [(run_id, str(path), int(row["line_no"]), reason[i],
                    json.dumps({c: (None if pd.isna(row[c]) else row[c]) for c in df.columns if c != "line_no"}))
                   for i, row in df[bad].iterrows()]
        rejects += [(run_id, str(path), line_no, "malformed_row", raw) for line_no, raw in malformed]
        good_rows = [(r.txn_id, r.customer_id, float(r.amount), r.status, r.event_ts, run_id, str(path))
                     for r in good.itertuples()]

        rows_in = len(df) + len(malformed)
        profile = make_profile(df, rows_in, as_of)
        alerts = []
        if not is_baseline:
            row = conn.execute("SELECT profile_json FROM baseline ORDER BY id DESC LIMIT 1").fetchone()
            if row:
                alerts = compare(json.loads(row[0]), profile, len(rejects))
            else:
                alerts = [{"rule": "no_baseline", "column": "*",
                           "message": "No baseline yet. Run once with --baseline."}]

        with conn:  # one transaction: everything is saved, or nothing is
            # upsert, but never let an older version overwrite a newer one (out-of-order data)
            conn.executemany(
                "INSERT INTO transactions VALUES (?,?,?,?,?,?,?) ON CONFLICT(txn_id) DO UPDATE SET "
                "customer_id=excluded.customer_id, amount=excluded.amount, status=excluded.status, "
                "event_ts=excluded.event_ts, run_id=excluded.run_id, source_file=excluded.source_file "
                "WHERE excluded.event_ts >= transactions.event_ts",
                good_rows)
            conn.executemany("INSERT INTO rejects VALUES (?,?,?,?,?)", rejects)
            conn.executemany("INSERT INTO alerts VALUES (?,?,?,?)",
                             [(run_id, a["rule"], a["column"], a["message"]) for a in alerts])
            if is_baseline:
                conn.execute("INSERT INTO baseline(profile_json) VALUES (?)", (json.dumps(profile),))
            # reconciliation: every input row is either loaded or rejected
            assert rows_in == len(good) + len(rejects), "row counts do not reconcile"
            conn.execute("UPDATE runs SET status='SUCCESS', ended_at=?, rows_in=?, rows_valid=?, "
                         "rows_rejected=?, alerts=? WHERE run_id=?",
                         (now_iso(), rows_in, len(good), len(rejects), len(alerts), run_id))
        result.update(status="SUCCESS", run_id=run_id, rows_in=rows_in, rows_valid=len(good),
                      rows_rejected=len(rejects), alerts=alerts)
        log.info("[%s] SUCCESS in=%d valid=%d rejected=%d alerts=%d", run_id, rows_in, len(good),
                 len(rejects), len(alerts))
    except Exception as exc:  # nothing was published; record why
        conn.rollback()
        with conn:
            conn.execute("UPDATE runs SET status='FAILED', ended_at=?, failure_reason=? WHERE run_id=?",
                         (now_iso(), f"{type(exc).__name__}: {exc}", run_id))
        result.update(status="FAILED", run_id=run_id, failure_reason=str(exc))
        log.error("[%s] FAILED %s: %s", run_id, type(exc).__name__, exc)
    finally:
        conn.close()
    return result


# ---------------------------------------------------------------- 6. sample data generator
def write_customers(path, count=500):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id"])
        for i in range(1, count + 1):
            w.writerow([f"C{i:05d}"])


def generate(path, rows=1000, seed=42, day="2026-10-01", scenario="normal", customers=500):
    """scenario: normal | drift | dirty | stale. Fixed seed => same file every time."""
    rnd = random.Random(seed)
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    mu, failed = (4.3, 0.30) if scenario == "drift" else (3.5, 0.07)
    shift = timedelta(days=3) if scenario == "stale" else timedelta(0)
    out = []
    reasons = [""] * rows          # expected reject reason per row (the answer key)
    for i in range(rows):
        status = rnd.choices(["completed", "pending", "failed", "refunded"],
                             [0.83 - failed, 0.10, failed, 0.07])[0]
        when = start + timedelta(seconds=rnd.randrange(86400)) - shift
        out.append([f"T{day.replace('-', '')}{i:06d}", f"C{rnd.randint(1, customers):05d}",
                    f"{rnd.lognormvariate(mu, 0.8):.2f}", status, when.strftime(TS_FORMAT)])
    if scenario == "dirty":
        out[0][1] = ""                       # missing customer
        out[1][2] = "abc"                    # invalid amount
        out[2][2] = "-5.00"                  # negative amount
        out[3][3] = "unknown"                # invalid status
        out[4][4] = "not-a-date"             # invalid timestamp
        out[5][1] = "C99999"                 # customer that does not exist
        out[6] = out[6][:3]                  # malformed (too few fields)
        out.append(list(out[7]))             # duplicate txn_id
        reasons[:7] = ["missing_customer_id", "invalid_amount", "negative_amount", "invalid_status",
                       "invalid_event_ts", "unknown_customer", "malformed_row"]
        reasons.append("duplicate_txn_id")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(REQUIRED)
        w.writerows(out)
    # answer key lives in a SEPARATE file, never in the input data
    labels_file = Path(path).with_name(Path(path).stem + ".labels.csv")
    with open(labels_file, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["line_no", "expected_reason"])
        w.writerows([(n + 2, why) for n, why in enumerate(reasons) if why])


# ---------------------------------------------------------------- 7. command line + demo
def demo():
    folder, db = Path("sample_data"), "output/dq.sqlite"
    Path(db).unlink(missing_ok=True)
    write_customers(folder / "customers.csv")
    plan = [("day1_baseline", "normal", True), ("day2_normal", "normal", False),
            ("day3_drift", "drift", False), ("day4_dirty", "dirty", False),
            ("day5_stale", "stale", False)]
    for n, (name, scenario, is_base) in enumerate(plan, start=1):
        day = f"2026-10-{n:02d}"
        generate(folder / f"{name}.csv", 1000, seed=n, day=day, scenario=scenario)
        res = run_batch(folder / f"{name}.csv", db, folder / "customers.csv",
                        as_of=f"2026-10-{n + 1:02d}T01:00:00Z", is_baseline=is_base)
        print(f"\n{name}: {res['status']}  in={res['rows_in']} valid={res['rows_valid']} "
              f"rejected={res['rows_rejected']}  alerts={len(res['alerts'])}")
        for a in res["alerts"]:
            print(f"   - [{a['rule']}] {a['message']}")
    again = run_batch(folder / "day2_normal.csv", db, folder / "customers.csv", as_of="2026-10-03T01:00:00Z")
    print(f"\nrerun day2_normal: {again['status']} ({again.get('note')})")

    conn = sqlite3.connect(db)
    print("\nReject reasons:", dict(conn.execute("SELECT reason, COUNT(*) FROM rejects GROUP BY reason")))
    print("Rows in transactions:", conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
    write_validation_report(db, folder)
    export_csv(db)


def write_validation_report(db_path, folder, out_dir="output"):
    """Reconciliation queries + comparison with the answer keys. Returns True if all checks pass."""
    conn = sqlite3.connect(db_path)
    lines, results = ["# Validation report", "", "## 1. Reconciliation per successful run", ""], []

    def check(text, passed):
        results.append(passed)
        lines.append(f"- {'PASS' if passed else 'FAIL'}: {text}")

    runs = conn.execute("SELECT run_id, source_file, rows_in, rows_valid, rows_rejected FROM runs "
                        "WHERE status='SUCCESS' ORDER BY started_at").fetchall()
    for run_id, src, rows_in, valid, rejected in runs:
        in_rejects = conn.execute("SELECT COUNT(*) FROM rejects WHERE run_id=?", (run_id,)).fetchone()[0]
        check(f"{Path(src).name}: rows_in {rows_in} = valid {valid} + rejected {rejected}; "
              f"rejects table has {in_rejects}", rows_in == valid + rejected and in_rejects == rejected)

    lines += ["", "## 2. Key totals", ""]
    loaded = sum(r[3] for r in runs)
    stored = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    check(f"valid rows loaded across runs ({loaded}) = rows in transactions ({stored})", loaded == stored)
    orphans = conn.execute("SELECT COUNT(*) FROM transactions t LEFT JOIN customers c "
                           "ON t.customer_id=c.customer_id WHERE c.customer_id IS NULL").fetchone()[0]
    check(f"transactions with an unknown customer: {orphans} (expected 0)", orphans == 0)

    lines += ["", "## 3. Rejects vs expected answer keys", ""]
    for key in sorted(Path(folder).glob("*.labels.csv")):
        source = str(Path(folder) / (key.name[:-len(".labels.csv")] + ".csv"))
        if not conn.execute("SELECT 1 FROM runs WHERE source_file=? AND status='SUCCESS'", (source,)).fetchone():
            continue
        with open(key, newline="") as f:
            expected = {int(r["line_no"]): r["expected_reason"] for r in csv.DictReader(f)}
        actual = dict(conn.execute("SELECT line_no, reason FROM rejects WHERE source_file=?", (source,)))
        check(f"{Path(source).name}: {len(actual)} rejects found, {len(expected)} expected", actual == expected)

    lines += ["", f"**{sum(results)} of {len(results)} checks passed.**"]
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    (Path(out_dir) / "validation_report.md").write_text("\n".join(lines), encoding="utf-8")
    conn.close()
    print(f"Validation report: {sum(results)}/{len(results)} checks passed -> {out_dir}/validation_report.md")
    return all(results)


def benchmark(rows=100_000):
    """Time a full load, then compare one-commit-per-row with one bulk insert."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        write_customers(tmp / "customers.csv")
        generate(tmp / "big.csv", rows, seed=1)
        start = time.perf_counter()
        res = run_batch(tmp / "big.csv", str(tmp / "b.db"), tmp / "customers.csv",
                        as_of="2026-10-02T01:00:00Z", is_baseline=True)
        secs = time.perf_counter() - start
    print(f"Loaded {res['rows_in']:,} rows in {secs:.2f}s = {res['rows_in'] / secs:,.0f} rows/s")

    data = [(f"T{i}", "C1", 1.0, "completed", "2026-10-01T00:00:00Z", "r", "f") for i in range(5000)]
    ddl = ("CREATE TABLE t(txn_id TEXT PRIMARY KEY, customer_id, amount, status, event_ts, "
           "run_id, source_file)")
    with tempfile.TemporaryDirectory() as tmp:
        slow_db, fast_db = sqlite3.connect(Path(tmp) / "a.db"), sqlite3.connect(Path(tmp) / "b.db")
        slow_db.execute(ddl)
        fast_db.execute(ddl)
        t = time.perf_counter()
        for r in data:                       # slow: one commit per row
            slow_db.execute("INSERT INTO t VALUES (?,?,?,?,?,?,?)", r)
            slow_db.commit()
        slow = time.perf_counter() - t
        t = time.perf_counter()
        with fast_db:                        # fast: one transaction, one executemany
            fast_db.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?)", data)
        fast = time.perf_counter() - t
        slow_db.close()
        fast_db.close()
    print(f"Optimisation (5,000 rows): commit per row {slow:.2f}s vs one bulk insert {fast:.3f}s "
          f"= {slow / fast:.0f}x faster. The pipeline uses the bulk insert.")


def setup_logging(console):
    Path("output").mkdir(exist_ok=True)
    handlers = [logging.FileHandler("output/pipeline.log", encoding="utf-8")]
    if console:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True,
                        format="%(asctime)s %(levelname)s %(message)s")


def export_csv(db_path, out_dir="output"):
    """Write the result tables to CSV files so they can be opened in Excel."""
    conn = sqlite3.connect(db_path)
    for table in ["runs", "alerts", "rejects", "transactions"]:
        cursor = conn.execute(f"SELECT * FROM {table}")
        with open(Path(out_dir) / f"{table}.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([d[0] for d in cursor.description])
            writer.writerows(cursor)
    print(f"CSV files written to {out_dir}/: runs.csv, alerts.csv, rejects.csv, transactions.csv")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("demo")
    g = sub.add_parser("generate")
    g.add_argument("--out", required=True)
    g.add_argument("--rows", type=int, default=1000)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--day", default="2026-10-01")
    g.add_argument("--scenario", default="normal")
    b = sub.add_parser("bench")
    b.add_argument("--rows", type=int, default=100_000)
    r = sub.add_parser("run")
    r.add_argument("--file", required=True)
    r.add_argument("--db", default="output/dq.sqlite")
    r.add_argument("--customers")
    r.add_argument("--as-of")
    r.add_argument("--baseline", action="store_true")
    a = parser.parse_args()
    a.cmd = a.cmd or "demo"  # no command given: run the demo

    setup_logging(console=(a.cmd == "run"))
    if a.cmd == "demo":
        demo()
    elif a.cmd == "bench":
        benchmark(a.rows)
    elif a.cmd == "generate":
        write_customers(Path(a.out).parent / "customers.csv")
        generate(a.out, a.rows, a.seed, a.day, a.scenario)
        print("written", a.out)
    else:
        res = run_batch(a.file, a.db, a.customers, a.as_of, a.baseline)
        print(res["status"], {k: v for k, v in res.items() if k != "alerts"})
        for al in res["alerts"]:
            print(f"  - [{al['rule']}] {al['message']}")
        return 1 if res["status"] == "FAILED" else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
