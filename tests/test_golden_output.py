"""
Golden-file tests for generated DAB output.

Each case generates DAB files and compares them byte for byte with the
committed files under tests/golden/<case_id>/. Any change to generated output
fails these tests, including renamed resources, which make `bundle deploy`
recreate pipelines and lose data.

After an intended output change, regenerate the golden files, review the
diff, and add a CHANGELOG entry:

    TAPWORKS_UPDATE_GOLDEN=1 python3 -m pytest tests/test_golden_output.py
"""

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

from tapworks.core.runner import run_pipeline_generation


PROJECT_ROOT = Path(__file__).parent.parent
GOLDEN_DIR = Path(__file__).parent / 'golden'
UPDATE_GOLDEN = os.environ.get('TAPWORKS_UPDATE_GOLDEN') == '1'

CONNECTOR_EXAMPLES = PROJECT_ROOT / 'examples' / 'connectors'
GROUP_EXAMPLES = PROJECT_ROOT / 'examples' / 'features' / 'group_based_config'

TARGETS = {
    'dev': {'workspace_host': 'https://dev.cloud.databricks.com'},
    'prod': {
        'workspace_host': 'https://prod.cloud.databricks.com',
        'root_path': '/Shared/tapworks/prod',
    },
}


def _large_df(num_rows: int) -> pd.DataFrame:
    """Single-prefix table list large enough to split across pipelines/gateways."""
    return pd.DataFrame({
        'project_name': ['large_project'] * num_rows,
        'source_database': ['SourceDB'] * num_rows,
        'source_schema': ['dbo'] * num_rows,
        'source_table_name': [f'Table_{i:04d}' for i in range(num_rows)],
        'target_catalog': ['main'] * num_rows,
        'target_schema': ['bronze'] * num_rows,
        'target_table_name': [f'table_{i:04d}' for i in range(num_rows)],
        'pipeline_catalog': ['main'] * num_rows,
        'pipeline_schema': ['bronze'] * num_rows,
        'connection_name': ['conn'] * num_rows,
        'prefix': ['sales'] * num_rows,
    })


# Group-based examples: (folder, connector, default_values, override_config),
# mirroring examples/features/group_based_config/example_notebook.ipynb
GROUP_CASES = [
    ('01_global_defaults', 'salesforce', {'schedule': '0 */6 * * *'}, None),
    ('02_prefix_based', 'salesforce', {
        '*': {'schedule': '0 */6 * * *'},
        'sales': {'schedule': '*/15 * * * *'},
        'hr': {'schedule': '0 0 * * *'},
    }, None),
    ('03_pipeline_group_specific', 'salesforce', {
        '*': {'schedule': '0 */6 * * *'},
        'sales': {'schedule': '*/15 * * * *'},
        'sales_2': {'schedule': '*/30 * * * *'},
        'sales_3': {'schedule': '0 * * * *'},
    }, None),
    ('04_group_overrides', 'salesforce', {
        '*': {'schedule': '*/15 * * * *'},
    }, {
        '*': {'pause_status': 'UNPAUSED'},
        'finance': {'pause_status': 'PAUSED'},
    }),
    ('05_database_pipelines', 'sql_server', {
        '*': {'schedule': '0 */6 * * *'},
        'sales': {'schedule': '*/15 * * * *'},
        'hr': {'schedule': '0 0 * * *'},
    }, None),
    ('06_database_gateways', 'sql_server', {
        '*': {'schedule': '0 */6 * * *'},
        'sales': {'schedule': '*/15 * * * *'},
        'sales_2': {'schedule': '*/30 * * * *'},
        'hr': {'schedule': '0 0 * * *'},
        'finance': {'schedule': '0 */12 * * *'},
    }, {
        '*': {'pause_status': 'UNPAUSED'},
        'finance': {'pause_status': 'PAUSED'},
    }),
    ('07_mixed_values', 'sql_server', {
        '*': {'schedule': '0 */6 * * *'},
        'sales': {'schedule': '*/15 * * * *'},
        'hr': {'schedule': '0 0 * * *'},
        'finance': {'schedule': '0 */4 * * *'},
    }, None),
    ('08_connection_names', 'sql_server', {
        '*': {'connection_name': 'default_sql_connection', 'schedule': '0 */6 * * *'},
        'sales': {'connection_name': 'sales_db_connection'},
        'hr': {'connection_name': 'hr_db_connection'},
        'finance': {'connection_name': 'finance_db_connection'},
    }, None),
    ('09_gateway_driver_types', 'sql_server', {
        'sales': {
            'gateway_driver_type': 'c5a.8xlarge',
            'gateway_worker_type': 'c5a.4xlarge',
        },
        'finance': {'gateway_driver_type': 'c5a.4xlarge'},
    }, None),
]


def _build_cases():
    """Return {case_id: generate(output_dir)}."""
    cases = {}

    for csv_path in sorted(CONNECTOR_EXAMPLES.glob('*/basic/pipeline_config.csv')):
        connector_name = csv_path.parts[-3]
        cases[f'example_{connector_name}'] = (
            lambda out, c=connector_name, p=csv_path: run_pipeline_generation(
                connector_name=c, input_source=str(p), output_dir=out, targets=TARGETS,
            )
        )

    for folder, connector_name, defaults, overrides in GROUP_CASES:
        cases[f'group_{folder}'] = (
            lambda out, c=connector_name, f=folder, d=defaults, o=overrides: run_pipeline_generation(
                connector_name=c,
                input_source=str(GROUP_EXAMPLES / f / 'pipeline_config.csv'),
                output_dir=out,
                targets=TARGETS,
                default_values=d,
                override_config=o,
            )
        )

    # Load balancing with default limits (250 per pipeline / gateway)
    for connector_name in ('salesforce', 'sql_server', 'oracle_integrated'):
        cases[f'load_balancing_{connector_name}_600_defaults'] = (
            lambda out, c=connector_name: run_pipeline_generation(
                connector_name=c, input_source=_large_df(600), output_dir=out, targets=TARGETS,
            )
        )

    # Several pipelines per gateway
    cases['load_balancing_sql_server_600_gw500_p250'] = (
        lambda out: run_pipeline_generation(
            connector_name='sql_server',
            input_source=_large_df(600),
            output_dir=out,
            targets=TARGETS,
            max_tables_per_gateway=500,
            max_tables_per_pipeline=250,
        )
    )

    return cases


CASES = _build_cases()


def _read_tree(root: Path) -> dict:
    """Return {relative_path: content} for all files under root."""
    return {
        str(path.relative_to(root)): path.read_text()
        for path in sorted(root.rglob('*'))
        if path.is_file()
    }


@pytest.mark.parametrize('case_id', sorted(CASES))
def test_generated_output_matches_golden(case_id, tmp_path):
    output_dir = tmp_path / 'output'
    CASES[case_id](str(output_dir))
    actual = _read_tree(output_dir)
    golden_case_dir = GOLDEN_DIR / case_id

    if UPDATE_GOLDEN:
        shutil.rmtree(golden_case_dir, ignore_errors=True)
        shutil.copytree(output_dir, golden_case_dir)
        return

    assert golden_case_dir.exists(), (
        f"No golden files for '{case_id}'. Run with TAPWORKS_UPDATE_GOLDEN=1 to create them."
    )
    expected = _read_tree(golden_case_dir)

    assert sorted(actual) == sorted(expected), f"Generated file set changed for '{case_id}'"
    for rel_path, content in expected.items():
        assert actual[rel_path] == content, (
            f"Generated output changed: {case_id}/{rel_path}. If intended, regenerate with "
            f"TAPWORKS_UPDATE_GOLDEN=1 and add a CHANGELOG entry."
        )
