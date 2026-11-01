# Data-Quality Monitor

A small Python pipeline that loads daily CSV batches of transactions, rejects bad rows into a
traceable table, and compares each batch with a stored **baseline** to raise **explainable alerts**
about volume, null rate, value distribution and freshness changes.

## Files
| file | purpose |
|---|---|
| `simple_dq.py` | the whole pipeline (validate, profile, compare, load, generate sample data, report, benchmark) |
| `test_simple_dq.py` | 18 automated tests |
| `dq_monitor.ipynb` | the same code as a Jupyter notebook |
| `DESIGN.md` | short design and trade-off note |
| `requirements.txt` | `pandas`, `numpy`, `pytest` |
| `sample_data/` | synthetic input batches and their separate answer keys (`*.labels.csv`) |
| `output/` | sample output from `demo` (database, CSVs, validation report, log) |

## Setup
```
pip install -r requirements.txt
```

## Run
```
python simple_dq.py demo          # or just: python simple_dq.py
python -m pytest -q               # run the tests
python simple_dq.py bench         # performance measurement (100,000 rows)
```
`demo` creates 5 sample days, loads them, prints the alerts, shows that a rerun is skipped and
writes the results to `output/`.

Run your own files:
```
python simple_dq.py generate --out data/b1.csv --rows 100000 --day 2026-10-01 --seed 1
python simple_dq.py run --file data/b1.csv --customers data/customers.csv --as-of 2026-10-02T01:00:00Z --baseline
python simple_dq.py generate --out data/b2.csv --rows 100000 --day 2026-10-02 --scenario drift --seed 2
python simple_dq.py run --file data/b2.csv --as-of 2026-10-03T01:00:00Z
```
Scenarios: `normal`, `drift`, `dirty`, `stale`. `--baseline` marks a batch as the reference for later
comparisons. `--as-of` is the load time, used to measure freshness (default: now).

## Output (in `output/`)
`dq.sqlite` (all tables), `runs.csv`, `alerts.csv`, `rejects.csv`, `transactions.csv`,
`validation_report.md` (reconciliation results) and `pipeline.log` (run start, finish or failure
with run_id and counts).

## How it works
```
CSV file -> read_batch -> check_row (per row) --bad--> rejects table
                                   |--good--> transactions table
                      make_profile -> compare with baseline -> alerts table
                      run status, counts and failure reason -> runs table
```
1. **Read**: the CSV becomes a pandas DataFrame of text; rows with the wrong number of fields are "malformed" and set aside.
2. **Validate**: DataFrame checks (`isna()` for nulls, `to_numeric`, `to_datetime`, `isin`, `duplicated`) give each row at most one reject reason.
3. **Profile**: row count, null rate per column, amount mean/std, status mix, newest event time.
4. **Compare**: the profile is checked against the latest baseline (rules below).
5. **Load**: everything is saved in one database transaction, then the run is marked SUCCESS.

## Data dictionary
Source file `transactions.csv` (one row per transaction, key `txn_id`):

| column | type | rule |
|---|---|---|
| txn_id | text | required, unique |
| customer_id | text | required, must exist in customers.csv |
| amount | number | required, numeric, >= 0 |
| status | text | required: completed, pending, failed, refunded |
| event_ts | ISO-8601 UTC | required, format `YYYY-MM-DDTHH:MM:SSZ` |

Tables: `customers`, `transactions`, `rejects`, `baseline`, `alerts`, `runs`.
Every loaded or rejected row carries `run_id` and `source_file`; rejects also keep `line_no` and the raw row.

Reject reasons: `missing_<column>`, `invalid_amount`, `negative_amount`, `invalid_status`,
`invalid_event_ts`, `unknown_customer`, `duplicate_txn_id`, `malformed_row`.
Column and table names use snake_case, timestamps are ISO-8601 UTC, ids are deterministic
(`batch_id` is a hash of the file contents).

## Alert rules (thresholds are constants at the top of `simple_dq.py`)
| rule | fires when |
|---|---|
| volume_change | rows are below 50% or above 200% of the baseline |
| empty_batch | file has no data rows |
| reject_rate | more than 5% of rows rejected |
| null_rate_increase | a column's null rate rose by more than 2 points |
| amount_mean_shift | amount mean moved by more than 0.5 baseline standard deviations |
| status_mix_change | a status share changed by more than 10 points |
| stale_data | newest event is more than 24 hours old at load time |

Example alert: `status 'failed' went from 7.4% to 32.4% of rows.`

## Reruns, failures and out-of-order data
- **Rerun of the same file**: `batch_id` is a hash of the file, so an already loaded file is skipped.
- **Failure and retry**: if anything goes wrong (for example a required column is missing), nothing is
  saved from that file, the run is marked FAILED with the reason, and running the same command again retries it.
- **Backfill / late or out-of-order data**: files can be loaded in any order; loading is an upsert on
  `txn_id` and an older `event_ts` never overwrites a newer one.
- **Reconciliation**: each load asserts rows read = rows loaded + rows rejected.

## Validation and evidence
- **Answer keys**: `generate` writes the expected reject reasons to a separate `<batch>.labels.csv`; the pipeline never reads it.
- **Validation report** (`output/validation_report.md`, 12 checks in the demo): rows in = valid + rejected
  per run; rejects table count matches; valid rows loaded = rows in `transactions`; no transaction has an
  unknown customer; actual rejects (line number and reason) equal the answer key.
- **Logging and metrics**: `runs` table (run_id, start/end time, rows in/valid/rejected, alert count,
  failure reason) plus `output/pipeline.log`.

## Tests
`python -m pytest -q` (18 tests) covers: normal load, empty file, header-only file, malformed row, each
row rule (nulls, bad amount/status/date, unknown customer), boundary amount (0), duplicate key, missing
column, rerun, older-version-does-not-overwrite, no false alerts on a normal batch, alerts on drift /
stale / empty batches, generator reproducibility, rejects matching the independent answer key, and the
validation report passing.

## Performance
`python simple_dq.py bench` loads 100,000 rows in about 2 seconds (about 45,000 to 60,000 rows/s on the test machine; results vary). **Optimisation used**: all rows are saved with one `executemany` inside one
transaction. In the same benchmark, one commit per row was about 200 to 300 times slower on 5,000 rows.

## Assumptions and limitations
- The baseline is one batch.
- Batches are assumed to cover the same time period, so row counts are comparable.
- Duplicates inside a file keep the first row; across files the newest `event_ts` wins.
- SQLite supports one writer at a time; there is no scheduler.
- Sample data is synthetic and generated with a fixed seed, so results are reproducible.

## Possible next steps
Rolling baselines, a config file for thresholds, Parquet output, scheduling with Airflow, Pipeline can be created by using ADF with Azure Cloud,sending alerts by email or Slack.
