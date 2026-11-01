import csv
import sqlite3

import pytest

from simple_dq import REQUIRED, generate, run_batch, write_customers, write_validation_report

AS_OF = "2026-10-02T01:00:00Z"


def row(i, **kw):
    r = dict(txn_id=f"T{i:04d}", customer_id="C00001", amount="10.50", status="completed",
             event_ts="2026-10-01T10:00:00Z")
    r.update(kw)
    return [r[c] for c in REQUIRED]


@pytest.fixture
def env(tmp_path):
    write_customers(tmp_path / "customers.csv", 5)
    return tmp_path, str(tmp_path / "t.db"), str(tmp_path / "customers.csv")


def load(env, rows, name="b.csv", header=REQUIRED, **kw):
    tmp, db, cust = env
    with open(tmp / name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return run_batch(tmp / name, db, cust, as_of=AS_OF, **kw)


def sql(env, query):
    return sqlite3.connect(env[1]).execute(query).fetchall()


def test_happy_path(env):
    res = load(env, [row(i) for i in range(10)])
    assert (res["status"], res["rows_valid"], res["rows_rejected"]) == ("SUCCESS", 10, 0)


def test_empty_file_and_header_only(env):
    (env[0] / "zero.csv").write_text("")
    assert run_batch(env[0] / "zero.csv", env[1], env[2], as_of=AS_OF)["status"] == "SUCCESS"
    assert load(env, [], name="h.csv")["rows_in"] == 0


def test_malformed_row_rejected(env):
    res = load(env, [row(1), ["T2", "C00001"]])
    assert res["rows_rejected"] == 1
    assert sql(env, "SELECT reason FROM rejects") == [("malformed_row",)]


@pytest.mark.parametrize("index,value,reason", [
    (1, "", "missing_customer_id"), (1, "C99999", "unknown_customer"), (2, "abc", "invalid_amount"),
    (2, "-1", "negative_amount"), (3, "bad", "invalid_status"), (4, "yesterday", "invalid_event_ts")])
def test_row_rules(env, index, value, reason):
    bad = row(2)
    bad[index] = value
    load(env, [row(1), bad])
    assert sql(env, "SELECT reason FROM rejects") == [(reason,)]


def test_boundary_amount_zero_is_valid(env):
    assert load(env, [row(1, amount="0")])["rows_valid"] == 1


def test_duplicate_keeps_first(env):
    res = load(env, [row(1, amount="1"), row(1, amount="2")])
    assert res["rows_rejected"] == 1
    assert sql(env, "SELECT amount FROM transactions") == [(1.0,)]


def test_missing_column_fails_and_loads_nothing(env):
    res = load(env, [["T1", "C00001", "5", "completed"]], header=REQUIRED[:-1])
    assert res["status"] == "FAILED"
    assert sql(env, "SELECT COUNT(*) FROM transactions") == [(0,)]
    assert sql(env, "SELECT status FROM runs") == [("FAILED",)]


def test_rerun_same_file_is_skipped(env):
    rows = [row(i) for i in range(5)]
    assert load(env, rows)["status"] == "SUCCESS"
    assert load(env, rows)["status"] == "SKIPPED"
    assert sql(env, "SELECT COUNT(*) FROM transactions") == [(5,)]


def test_older_version_does_not_overwrite_newer(env):
    load(env, [row(1, amount="100", event_ts="2026-10-01T12:00:00Z")], name="new.csv")
    load(env, [row(1, amount="1", event_ts="2026-10-01T08:00:00Z")], name="old.csv")
    assert sql(env, "SELECT amount FROM transactions") == [(100.0,)]


def make_batch(tmp, name, scenario, seed, day):
    generate(tmp / name, 1000, seed, day, scenario)
    return tmp / name


def test_alerts(tmp_path):
    write_customers(tmp_path / "c.csv")
    db, cust = str(tmp_path / "d.db"), str(tmp_path / "c.csv")
    base = make_batch(tmp_path, "base.csv", "normal", 1, "2026-10-01")
    run_batch(base, db, cust, as_of=AS_OF, is_baseline=True)
    ok = run_batch(make_batch(tmp_path, "ok.csv", "normal", 2, "2026-10-01"), db, cust, as_of=AS_OF)
    assert ok["alerts"] == []                                        # no false alarm
    drift = run_batch(make_batch(tmp_path, "d.csv", "drift", 3, "2026-10-01"), db, cust, as_of=AS_OF)
    assert {"amount_mean_shift", "status_mix_change"} <= {a["rule"] for a in drift["alerts"]}
    stale = run_batch(make_batch(tmp_path, "s.csv", "stale", 4, "2026-10-01"), db, cust, as_of=AS_OF)
    assert "stale_data" in {a["rule"] for a in stale["alerts"]}
    empty = tmp_path / "e.csv"
    empty.write_text(",".join(REQUIRED) + "\n")
    assert run_batch(empty, db, cust, as_of=AS_OF)["alerts"][0]["rule"] == "empty_batch"


def test_generator_is_reproducible(tmp_path):
    generate(tmp_path / "a.csv", 100, 7, scenario="dirty")
    generate(tmp_path / "b.csv", 100, 7, scenario="dirty")
    assert (tmp_path / "a.csv").read_bytes() == (tmp_path / "b.csv").read_bytes()


def test_rejects_match_expected_answer_key(tmp_path):
    write_customers(tmp_path / "c.csv")
    generate(tmp_path / "d.csv", 500, 3, scenario="dirty")
    db = str(tmp_path / "x.db")
    run_batch(tmp_path / "d.csv", db, tmp_path / "c.csv", as_of=AS_OF)
    with open(tmp_path / "d.labels.csv", newline="") as f:
        expected = {int(r["line_no"]): r["expected_reason"] for r in csv.DictReader(f)}
    assert dict(sqlite3.connect(db).execute("SELECT line_no, reason FROM rejects")) == expected


def test_validation_report_passes(tmp_path):
    write_customers(tmp_path / "c.csv")
    generate(tmp_path / "d.csv", 500, 3, scenario="dirty")
    generate(tmp_path / "n.csv", 500, 4, day="2026-10-02", scenario="normal")
    db = str(tmp_path / "x.db")
    for name in ["n", "d"]:
        run_batch(tmp_path / f"{name}.csv", db, tmp_path / "c.csv", as_of=AS_OF)
    assert write_validation_report(db, tmp_path, tmp_path / "out") is True
