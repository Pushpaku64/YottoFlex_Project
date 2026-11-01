# Submission checklist

## 1. Make it run
```
pip install -r requirements.txt
python simple_dq.py demo
python -m pytest -q
python simple_dq.py bench
```
Expected: demo prints alerts for the drift and stale days, `Validation report: 12/12 checks passed`; pytest shows `18 passed`.

## 2. Public GitHub repository
1. Create an empty **public** repository on github.com (no README).
2. In the project folder:
```
git init
git add requirements.txt .gitignore
git commit -m "chore: add requirements and gitignore"
git add simple_dq.py
git commit -m "feat: validation, profiling, baseline comparison and idempotent load"
git add test_simple_dq.py
git commit -m "test: happy path, empty, malformed, duplicates, nulls, drift and rerun cases"
git add sample_data output
git commit -m "docs: add synthetic sample input, answer keys and sample output"
git add README.md DESIGN.md dq_monitor.ipynb
git commit -m "docs: README, design note and notebook"
git add AI_USAGE.md PROMPT_HISTORY.md SUBMISSION.md
git commit -m "docs: AI usage disclosure and prompt history"
git branch -M main
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```
Make later fixes as small separate commits. Do not rewrite dates or history; timestamps are checked.

## 3. AI transparency
Complete `AI_USAGE.md` (the review checklist) and paste the full chat transcript into `PROMPT_HISTORY.md`.

## 4. Be ready to explain
Read `DESIGN.md`, then practise explaining: why a file hash is the batch id, why one transaction per file, why the upsert checks `event_ts`, how each alert rule works, and what the limitations are.
