#!/usr/bin/env python3
"""
Test script to verify pause_status functionality.
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utilities.config_utils import create_jobs

# Test 1: With pause_status column set to PAUSED
print("=" * 80)
print("TEST 1: Job with PAUSED status")
print("=" * 80)

test_data_paused = {
    'pipeline_group': ['sales_01', 'sales_01'],
    'schedule': ['0 */6 * * *', '0 */6 * * *'],
    'pause_status': ['PAUSED', 'PAUSED']
}

df_paused = pd.DataFrame(test_data_paused)
jobs_paused = create_jobs(df_paused, 'test_project', 'sfdc')

job_keys = list(jobs_paused['resources']['jobs'].keys())
if job_keys:
    job = jobs_paused['resources']['jobs'][job_keys[0]]
    if 'pause_status' in job and job['pause_status'] == 'PAUSED':
        print(f"✓ SUCCESS: Job has pause_status = {job['pause_status']}")
    else:
        print("✗ FAILED: Job missing or incorrect pause_status")
        sys.exit(1)

# Test 2: With pause_status column set to UNPAUSED
print("\n" + "=" * 80)
print("TEST 2: Job with UNPAUSED status")
print("=" * 80)

test_data_unpaused = {
    'pipeline_group': ['sales_02', 'sales_02'],
    'schedule': ['0 */6 * * *', '0 */6 * * *'],
    'pause_status': ['UNPAUSED', 'UNPAUSED']
}

df_unpaused = pd.DataFrame(test_data_unpaused)
jobs_unpaused = create_jobs(df_unpaused, 'test_project', 'sfdc')

job_keys2 = list(jobs_unpaused['resources']['jobs'].keys())
if job_keys2:
    job2 = jobs_unpaused['resources']['jobs'][job_keys2[0]]
    if 'pause_status' in job2 and job2['pause_status'] == 'UNPAUSED':
        print(f"✓ SUCCESS: Job has pause_status = {job2['pause_status']}")
    else:
        print("✗ FAILED: Job missing or incorrect pause_status")
        sys.exit(1)

# Test 3: Without pause_status column (should not have the field)
print("\n" + "=" * 80)
print("TEST 3: Job without pause_status (default behavior)")
print("=" * 80)

test_data_no_pause = {
    'pipeline_group': ['sales_03', 'sales_03'],
    'schedule': ['0 */6 * * *', '0 */6 * * *']
}

df_no_pause = pd.DataFrame(test_data_no_pause)
jobs_no_pause = create_jobs(df_no_pause, 'test_project', 'sfdc')

job_keys3 = list(jobs_no_pause['resources']['jobs'].keys())
if job_keys3:
    job3 = jobs_no_pause['resources']['jobs'][job_keys3[0]]
    if 'pause_status' not in job3:
        print("✓ SUCCESS: Job does not have pause_status (as expected)")
    else:
        print(f"✗ FAILED: Job has unexpected pause_status = {job3['pause_status']}")
        sys.exit(1)

# Test 4: With lowercase pause_status (should be converted to uppercase)
print("\n" + "=" * 80)
print("TEST 4: Job with lowercase pause_status (should convert to uppercase)")
print("=" * 80)

test_data_lowercase = {
    'pipeline_group': ['sales_04', 'sales_04'],
    'schedule': ['0 */6 * * *', '0 */6 * * *'],
    'pause_status': ['paused', 'paused']
}

df_lowercase = pd.DataFrame(test_data_lowercase)
jobs_lowercase = create_jobs(df_lowercase, 'test_project', 'sfdc')

job_keys4 = list(jobs_lowercase['resources']['jobs'].keys())
if job_keys4:
    job4 = jobs_lowercase['resources']['jobs'][job_keys4[0]]
    if 'pause_status' in job4 and job4['pause_status'] == 'PAUSED':
        print(f"✓ SUCCESS: Lowercase 'paused' converted to '{job4['pause_status']}'")
    else:
        print(f"✗ FAILED: pause_status not properly converted: {job4.get('pause_status', 'missing')}")
        sys.exit(1)

print("\n" + "=" * 80)
print("ALL TESTS PASSED!")
print("=" * 80)
print("\nUsage example:")
print("  # In your input processing:")
print("  default_values = {'pause_status': 'PAUSED'}")
print("  df = process_input_config(df, required_columns, default_values)")
print("  # Or via override:")
print("  override_input_config = {'pause_status': 'UNPAUSED'}")
print("  df = process_input_config(df, required_columns, override_input_config=override_input_config)")
