# Validation report

## 1. Reconciliation per successful run

- PASS: day1_baseline.csv: rows_in 1000 = valid 1000 + rejected 0; rejects table has 0
- PASS: day2_normal.csv: rows_in 1000 = valid 1000 + rejected 0; rejects table has 0
- PASS: day3_drift.csv: rows_in 1000 = valid 1000 + rejected 0; rejects table has 0
- PASS: day4_dirty.csv: rows_in 1001 = valid 993 + rejected 8; rejects table has 8
- PASS: day5_stale.csv: rows_in 1000 = valid 1000 + rejected 0; rejects table has 0

## 2. Key totals

- PASS: valid rows loaded across runs (4993) = rows in transactions (4993)
- PASS: transactions with an unknown customer: 0 (expected 0)

## 3. Rejects vs expected answer keys

- PASS: day1_baseline.csv: 0 rejects found, 0 expected
- PASS: day2_normal.csv: 0 rejects found, 0 expected
- PASS: day3_drift.csv: 0 rejects found, 0 expected
- PASS: day4_dirty.csv: 8 rejects found, 8 expected
- PASS: day5_stale.csv: 0 rejects found, 0 expected

**12 of 12 checks passed.**