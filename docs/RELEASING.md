# Releases and Backward Compatibility

How Tapworks protects existing users from breaking changes.

> **Status:** Proposed. Nothing below is in place yet. The repo currently has no tags, releases, changelog, or CI, and the docs tell users to `pip install -e .` from a clone of `main`, so every merge to `main` reaches users immediately.

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
- The first step is to tag the current `main` as `v0.1.0` **before** any refactor lands, so existing users have a known-good version to pin.

### How users pin a version

```bash
# pip from git
pip install "git+https://github.com/databricks-solutions/lakeflow-tapworks.git@v0.1.0"

# local clone
git checkout v0.1.0 && pip install -e .
```

In Databricks, point the Git folder at the release tag instead of `main`.

`README.md` and `docs/USAGE.md` should recommend installing a pinned tag rather than `main`.

## 2. Golden-file tests

Golden-file tests guard the generated output, which is where breaks cause data loss.

- Commit the generated YAML for every example CSV (`examples/connectors/*/basic`, `examples/features/group_based_config/*`) plus a few large load-balancing cases under `tests/golden/`.
- One test regenerates every case and compares the result **byte for byte** with the committed files. YAML is written with `sort_keys=False`, so key order is part of the output.
- An intended output change requires regenerating the golden files on purpose. The change then shows up in the PR diff for the reviewer, and it needs a changelog entry.
- Refactors must leave every golden file unchanged. New connectors only **add** golden files.

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

## Known pending output change

`max_tables_per_gateway` is currently ignored by the CLI, `run_pipeline_generation()`, and the notebook runner. `runner.py` checks `run_complete_pipeline_generation`'s signature for the parameter, but that method takes `**kwargs`, so the check is always false and the default of 250 always applies. Calling the connector directly is not affected.

Reproduce: `tapworks sql_server --max-tables-per-gateway 1` on the 3-table `examples/connectors/sql_server` CSV still produces one gateway.

The fix is to inspect `generate_pipeline_config` instead. However, users who deployed with a gateway limit below their table count would get more gateways after regenerating, which renames and recreates pipelines. Ship the fix in its own minor release with a "Changes to generated output" entry.
