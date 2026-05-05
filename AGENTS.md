# Agent Instructions

Instructions for AI assistants working on this codebase.

## Documentation Updates

When making major changes, update these files:

| File | Update when... |
|------|----------------|
| `docs/ARCHITECTURE.md` | Class hierarchy, core flow, or method signatures change |
| `docs/USAGE.md` | CLI commands, API parameters, or configuration options change |
| `docs/VALIDATIONS.md` | Validation rules, required fields, or naming conventions change |
| `prompts/` | File paths, import patterns, or connector development workflow changes |
| `examples/connectors/*/example_notebook.ipynb` | Entry function signature or import paths change |
| `examples/connectors/*/basic/pipeline_config.csv` | Required columns, column names, or default values change |


## Development Rules

- Do not pick default values for variables or Tapworks configs without confirming with the user first.
- When adding a new field to the config or defaults, check if any validations need to be added and confirm whether those should be implemented before writing them.

## Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## Simplicity First

Minimum code that solves the problem. Nothing speculative.

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## Key Paths

- **Package code**: `src/tapworks/`
- **Connectors**: `src/tapworks/connectors/`
- **Core logic**: `src/tapworks/core/connectors.py`
- **Examples**: `examples/connectors/` and `examples/features/`
- **Tests**: `tests/`

## Testing

Run tests before committing:
```bash
python3 -m pytest tests/ -v
```

## Adding Connectors

See `prompts/` folder for AI-assisted connector development guides.
