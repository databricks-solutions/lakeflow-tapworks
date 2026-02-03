## Pipeline generator CLI template

> **Audience: Agent (template/scaffold)** — Copy/adapt this skeleton into a real file at `<connector>/pipeline_generator.py`.
>
> This is not a step-by-step workflow; see `prompts/04_build_pipeline_generator_cli.md` for the human-facing instructions.

### Create: `<connector>/pipeline_generator.py`

Goal: a small CLI wrapper around `connector.run_complete_pipeline_generation(...)`.

#### Skeleton (pseudo-code)

```python
import argparse
from pathlib import Path

from utilities import load_input_csv
from <connector>.connector import <ConnectorClass>


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--workspace-host", required=True)
    parser.add_argument("--root-path", required=False, default="/Users/<you>/.bundle/${bundle.name}/${bundle.target}")
    parser.add_argument("--output-dir", required=False, default="output")
    args = parser.parse_args()

    df = load_input_csv(args.input_csv)
    connector = <ConnectorClass>()
    connector.run_complete_pipeline_generation(
        df=df,
        output_dir=args.output_dir,
        targets={"dev": {"workspace_host": args.workspace_host, "root_path": args.root_path}},
        default_values={"project_name": args.project_name},
    )

    print(f"\nNext steps:\n  cd {args.output_dir}/{args.project_name}\n  databricks bundle validate -t dev\n  databricks bundle deploy -t dev\n")


if __name__ == "__main__":
    main()
```

### Acceptance checklist
- Uses `utilities.load_input_csv`
- Uses connector class (no duplicated load-balancing logic)
- Prints validate/deploy commands

