#!/usr/bin/env python3
"""
Test script to verify unified resource naming across all connectors.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utilities.config_utils import generate_resource_names

print("=" * 80)
print("UNIFIED RESOURCE NAMING TEST")
print("=" * 80)

# Test parameters
test_cases = [
    ('sales_01', 'sfdc'),
    ('sales_01', 'sqlserver'),
    ('sales_01', 'ga4'),
    ('business_unit1_p1', 'sfdc'),
    ('business_unit1_p1', 'sqlserver'),
    ('business_unit1_p1', 'ga4'),
]

print("\n" + "-" * 80)
print("Testing naming consistency across connectors:")
print("-" * 80)

for pipeline_group, connector_type in test_cases:
    names = generate_resource_names(pipeline_group, connector_type)

    print(f"\n{connector_type.upper()} - {pipeline_group}:")
    print(f"  Pipeline Name (display):    {names['pipeline_name']}")
    print(f"  Pipeline Resource Name:     {names['pipeline_resource_name']}")
    print(f"  Job Name:                   {names['job_name']}")
    print(f"  Job Display Name:           {names['job_display_name']}")
    print(f"  Task Key:                   {names['task_key']}")

# Verify consistent patterns
print("\n" + "=" * 80)
print("PATTERN VALIDATION")
print("=" * 80)

errors = []

# Test 1: All task_keys should be the same
print("\nTest 1: Unified task_key across all connectors")
task_keys = [generate_resource_names('test_01', ct)['task_key']
             for ct in ['sfdc', 'sqlserver', 'ga4']]
if len(set(task_keys)) == 1 and task_keys[0] == 'run_pipeline':
    print("  ✓ All connectors use unified task_key: 'run_pipeline'")
else:
    errors.append("Task keys are not unified")
    print(f"  ✗ FAILED: Task keys differ: {task_keys}")

# Test 2: Pipeline resource names follow pattern
print("\nTest 2: Pipeline resource naming pattern")
for connector_type in ['sfdc', 'sqlserver', 'ga4']:
    names = generate_resource_names('test_01', connector_type)
    expected = f"pipeline_{connector_type}_test_01"
    if names['pipeline_resource_name'] == expected:
        print(f"  ✓ {connector_type}: {names['pipeline_resource_name']}")
    else:
        errors.append(f"{connector_type} pipeline resource name mismatch")
        print(f"  ✗ {connector_type}: expected {expected}, got {names['pipeline_resource_name']}")

# Test 3: Job names follow pattern
print("\nTest 3: Job naming pattern")
for connector_type in ['sfdc', 'sqlserver', 'ga4']:
    names = generate_resource_names('test_01', connector_type)
    expected = f"job_{connector_type}_test_01"
    if names['job_name'] == expected:
        print(f"  ✓ {connector_type}: {names['job_name']}")
    else:
        errors.append(f"{connector_type} job name mismatch")
        print(f"  ✗ {connector_type}: expected {expected}, got {names['job_name']}")

# Test 4: Display names use connector-specific formats
print("\nTest 4: Display name formats")
expected_displays = {
    'sfdc': 'SFDC Ingestion - test_01',
    'sqlserver': 'SQL Server Ingestion - test_01',
    'ga4': 'GA4 Ingestion - test_01'
}
for connector_type, expected in expected_displays.items():
    names = generate_resource_names('test_01', connector_type)
    if names['pipeline_name'] == expected:
        print(f"  ✓ {connector_type}: {names['pipeline_name']}")
    else:
        errors.append(f"{connector_type} pipeline display name mismatch")
        print(f"  ✗ {connector_type}: expected {expected}, got {names['pipeline_name']}")

# Test 5: Job display names follow pattern
print("\nTest 5: Job display name formats")
expected_job_displays = {
    'sfdc': 'SFDC Pipeline Scheduler - test_01',
    'sqlserver': 'SQL Server Pipeline Scheduler - test_01',
    'ga4': 'GA4 Pipeline Scheduler - test_01'
}
for connector_type, expected in expected_job_displays.items():
    names = generate_resource_names('test_01', connector_type)
    if names['job_display_name'] == expected:
        print(f"  ✓ {connector_type}: {names['job_display_name']}")
    else:
        errors.append(f"{connector_type} job display name mismatch")
        print(f"  ✗ {connector_type}: expected {expected}, got {names['job_display_name']}")

# Final results
print("\n" + "=" * 80)
if errors:
    print("TESTS FAILED")
    print("=" * 80)
    for error in errors:
        print(f"  ✗ {error}")
    sys.exit(1)
else:
    print("ALL TESTS PASSED!")
    print("=" * 80)
    print("\nUnified naming ensures:")
    print("  ✓ Consistent resource naming across all connectors")
    print("  ✓ Same task_key ('run_pipeline') for all")
    print("  ✓ Predictable resource name patterns")
    print("  ✓ Connector-specific display names")
