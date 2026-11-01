# AI usage disclosure

**Tool used:** Claude (Anthropic) in a chat window. The full conversation is in `PROMPT_HISTORY.md`.

## Where AI accelerated my work
- Turning the assignment brief into a design:alert message,    
idempotent load.
- Got an idea to make pipeline.

## Where I relied on it heavily
- Pipeline Creation as I am aware of pipeline creation by  using Azure Cloud.


## Problems found while building (honest log)
- A bug in the first generator (rows chosen for corruption overlapped) was found while running it and fixed.
- A test failed because two sample batches reused the same day, so transaction IDs collided; the test was corrected, not the pipeline.


## What I personally changed or validated
Checklist:
- [ ] I read `simple_dq.py` and can explain each function (`read_batch`, `check_row`, `make_profile`, `compare`, `run_batch`).
- [ ] Things I changed myself: ___ (for example: thresholds, column names, rules, Schema Definition).
- [ ] One test I broke on purpose to confirm it fails: ___
- [ ] Limitations I agree with: one-batch baseline, single writer SQLite, no scheduler.
