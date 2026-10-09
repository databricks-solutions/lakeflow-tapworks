# Releases and Backward Compatibility

How Tapworks protects existing users from breaking changes.

> **Status:**
> - **In place:** release `v0.1.0` (baseline), golden-file tests, `CHANGELOG.md`, and pinned-install instructions in `README.md` / `docs/USAGE.md`.
> - **Not yet in place:** CI.

## What "breaking" means for Tapworks

Tapworks generates Databricks Asset Bundles. The most damaging kind of break is a change to **generated output**, not to the Python API:

- **Resource name changes** (pipeline, gateway, or job keys/names). When a pipeline's resource changes, `databricks bundle deploy` deletes and recreates it, and **the ingested data is lost**. Names are derived from `project_name`, `prefix`, `subgroup`, row order, and the load-balancing limits, so a change to any of these can rename resources.
- **Changes to other generated YAML** (fields added, removed, or changed), which may alter pipeline behavior on the next deploy.
- **Interface changes** to the things users actually call: registry names (`sql_server`, `postgresql`, ...), CLI flags, `run_pipeline_generation()` parameters, CSV columns, and defaults.

Internal class names (`DatabaseConnector`, `SaaSConnector`, ...) are **not** a compatibility surface. Users select connectors by registry name, and nobody is building connectors outside this repo.

## 1. Versioned releases

- Tag releases on `main` as `vMAJOR.MINOR.PATCH` and publish a GitHub Release with notes for each.
- Follow semantic versioning. While the version is `0.x`:
  - **Minor** bump (`0.1` → `0.2`): may contain breaking or output-changing changes, which must be listed in the changelog.
  - **Patch** bump (`0.2.0` → `0.2.1`): fixes only, with no change to generated output for existing configs.
- Keep `version` in `pyproject.toml` in sync with the tag.
- `v0.1.0` is the baseline, tagged on `main` at `984d9c6` before any refactor, so existing users have a known-good version to pin.

### How users pin a version

```bash
# pip from git
pip install "git+https://github.com/databricks-solutions/lakeflow-tapworks.git@v0.2.0"

# local clone
git checkout v0.2.0 && pip install -e .
```

In Databricks, point the Git folder at the release tag instead of `main`.

`README.md` and `docs/USAGE.md` should recommend installing a pinned tag rather than `main`.

## 2. Golden-file tests

Golden-file tests guard the generated output, which is where breaks cause data loss.

- `tests/test_golden_output.py` generates the DAB files for every connector example (`examples/connectors/*/basic`), every group-based example (`examples/features/group_based_config/*`), and a few large load-balancing cases. It compares them **byte for byte** with the committed files under `tests/golden/<case_id>/`. YAML is written with `sort_keys=False`, so key order is part of the output.
- An intended output change requires regenerating the golden files on purpose:
  ```bash
  TAPWORKS_UPDATE_GOLDEN=1 python3 -m pytest tests/test_golden_output.py
  ```
  The change then shows up in the PR diff for the reviewer, and it needs a changelog entry.
- Refactors must leave every golden file unchanged. New connectors only **add** golden cases (add them to `_build_cases()`).

## 3. Changelog

Keep a `CHANGELOG.md` at the repo root. Each release has these sections:

- **Added** / **Changed** / **Fixed**
- **Changes to generated output**: anything that changes generated resources for existing configs, which users must act on. Each entry states who is affected and what to do (stay pinned, adjust config, or review `bundle deploy` before approving).

## 4. CI

Add a GitHub Actions workflow that runs `python3 -m pytest tests/` on every PR. This enforces the golden-file tests before merge.

## 5. User-side safety net

`databricks bundle deploy` lists the pipelines it will delete or recreate and asks for confirmation. After upgrading Tapworks, users should review that prompt rather than passing `--auto-approve`. Document this next to the upgrade instructions.

## Release checklist

1. All tests pass, including golden files.
2. Any golden-file change is intended and listed under "Changes to generated output".
3. `pyproject.toml` version bumped.
4. `CHANGELOG.md` updated.
5. Tag `vX.Y.Z` on `main` and push it.
6. Create a GitHub Release from the tag, using the changelog entry as the notes.

## Example: the gateway limit fix

In `v0.1.0`, `max_tables_per_gateway` was ignored by the CLI, `run_pipeline_generation()`, and the notebook runner. `runner.py` checked `run_complete_pipeline_generation`'s signature for the parameter, but that method takes `**kwargs`, so the check was always false and the default of 250 always applied.

The fix (inspecting `generate_pipeline_config` instead) is one line, but it changes generated output for anyone who had set the limit, so it ships with a "Changes to generated output" entry in `CHANGELOG.md`. It is covered by a runner-level test (`tests/test_unified_entry.py`) and a golden case that goes through the runner (`load_balancing_sql_server_600_gw500_p250`). Both fail without the fix.
