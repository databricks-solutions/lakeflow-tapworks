## Test plan

> **Audience: Human** — Copy this checklist into your PR description or an issue comment and mark it off while testing.

### 1) Generate bundle
- [ ] Run the connector CLI (`<connector>/pipeline_generator.py`) against an example CSV
- [ ] Confirm output exists at `output/<project_name>/`

### 2) Validate bundle
- [ ] `cd output/<project_name>`
- [ ] `databricks bundle validate -t dev`

### 3) Deploy bundle
- [ ] `databricks bundle deploy -t dev`

### 4) Run pipelines
- [ ] Start updates (use full refresh for first run or schema changes)
- [ ] Confirm pipeline state is `COMPLETED` (or expected steady-state)

### 5) Verify data
- [ ] Confirm target catalog/schema exist
- [ ] Query row counts for a few tables
- [ ] Confirm incremental runs pick up new records (if CDC/incremental applies)

### Notes / common gotchas
- UC table ownership conflicts → use a unique target schema for tests
- Salesforce include-columns filtering must include required system fields
- Postgres CDC requires logical decoding + replication privilege

