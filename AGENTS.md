# Agent Instructions

Instructions for AI assistants working on this codebase.

## Documentation Updates

When making major changes, update these files:

| File | Update when... |
|------|----------------|
| `docs/ARCHITECTURE.md` | Class hierarchy, core flow, or method signatures change |
| `docs/USAGE.md` | CLI commands, API parameters, or configuration options change |
| `prompts/` | File paths, import patterns, or connector development workflow changes |
| `examples/connectors/*/example_notebook.ipynb` | Entry function signature or import paths change |

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
