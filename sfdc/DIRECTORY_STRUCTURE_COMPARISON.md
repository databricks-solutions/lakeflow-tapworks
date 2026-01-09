# Directory Structure Comparison

**Branch:** `main`
**Date:** December 9, 2024

---

## Overview

This document compares the directory structures of the **SQL Server** and **SFDC** pipeline generators after implementing the prefix+priority grouping logic.

---

## SQL Server Directory Structure

```
sqlserver/
├── load_balancing/                    # Load balancing module
│   ├── examples/
│   │   └── example_config.csv         # Input: source tables with priority_flag
│   └── generate_pipeline_config.py    # Groups tables by database + priority
│
├── deployment/                         # YAML generation module
│   ├── examples/
│   │   ├── example_config.csv
│   │   ├── example_gateway_file.yml
│   │   └── example_pipeline_file.yml
│   └── generate_dab_yaml.py           # Generates gateways.yml + pipelines.yml
│
├── dab/                                # Additional utilities
│   ├── auto_balance_sqlserver.py      # Alternative auto-balancer
│   ├── CSV_MODE_README.md
│   ├── examples/
│   │   ├── example_input_minimal.csv
│   │   └── example_input_tables.csv
│   └── test_load_scoring.py
│
├── run_pipeline_generation.py         # Unified runner (all-in-one)
├── README.md                           # Documentation
└── requirements.txt                    # Python dependencies
```

**Key Files:**
- **7 directories**, **14 files**

---

## SFDC Directory Structure (NEW - After Updates)

```
sfdc/
├── load_balancing/                    # Load balancing module
│   ├── examples/
│   │   ├── example_config.csv         # Input: objects with prefix + priority
│   │   └── output_config.csv          # Output: with pipeline_group added
│   └── generate_pipeline_config.py    # Groups by prefix + priority
│
├── deployment/                         # YAML generation module (legacy files)
│   ├── examples/
│   │   ├── example_config.csv
│   │   ├── example_databricks.yml
│   │   ├── example_pipeline.yml
│   │   └── generated_config.csv
│   ├── generate_dab_yaml.py           # Generates sfdc_pipeline.yml
│   ├── auto_balance.py                # Legacy (should move to load_balancing)
│   ├── prework.py                     # Catalog/schema setup utility
│   ├── main.ipynb                     # Notebook for running in Databricks
│   └── resources/
│       └── sfdc_pipeline.yml          # Generated YAML output
│
├── dab_deployment/                    # Test deployment directory
│   ├── databricks.yml                 # DAB configuration
│   ├── generated_config.csv           # Intermediate config
│   └── resources/
│       └── sfdc_pipeline.yml          # Final YAML for deployment
│
├── run_pipeline_generation.py         # Unified runner (all-in-one)
├── README.md                           # Documentation (needs update)
├── README (1).md                       # Old README (can be deleted)
├── requirements.txt                    # Python dependencies
└── TEST_RESULTS.md                     # Test documentation (not in git)
```

**Key Files:**
- **8 directories**, **20 files**

---

## Side-by-Side Comparison

| Component | SQL Server | SFDC (Current) | Status |
|-----------|------------|----------------|--------|
| **load_balancing/** | ✅ Present | ✅ Present | ✅ Aligned |
| **deployment/** | ✅ Present | ✅ Present | ✅ Aligned |
| **dab/** | ✅ Present (utilities) | ❌ Missing | ⚠️ Could add |
| **run_pipeline_generation.py** | ✅ Present | ✅ Present | ✅ Aligned |
| **README.md** | ✅ Updated | ⚠️ Needs update | 🔧 TODO |
| **requirements.txt** | ✅ Present | ✅ Present | ✅ Aligned |
| **main.ipynb** | ❌ N/A | ✅ Present | ℹ️ SFDC-specific |
| **prework.py** | ❌ N/A | ✅ Present | ℹ️ SFDC-specific |

---

## Key Differences

### 1. **Grouping Logic**

| Aspect | SQL Server | SFDC |
|--------|------------|------|
| **Grouping Method** | Database + priority_flag | Prefix + priority |
| **Input Columns** | `source_database`, `priority_flag` | `prefix`, `priority` |
| **Output Column** | `pipeline_group` (numeric) | `pipeline_group` (string: prefix_priority) |
| **Example** | `pipeline_group: 1, 2, 3` | `pipeline_group: business_unit1_01, business_unit1_02` |

### 2. **Gateway Requirement**

| Aspect | SQL Server | SFDC |
|--------|------------|------|
| **Gateway Required** | ✅ Yes (on-premises) | ❌ No (SaaS) |
| **Output Files** | `gateways.yml` + `pipelines.yml` | `sfdc_pipeline.yml` only |
| **Gateway Column** | ✅ Present in output | ❌ Not needed |

### 3. **SFDC-Specific Files**

Files unique to SFDC:

- **`main.ipynb`**: Notebook for running in Databricks workspace
- **`prework.py`**: Utility to create catalog/schema (Salesforce-specific setup)
- **`dab_deployment/`**: Test deployment directory (created during testing)
- **`TEST_RESULTS.md`**: End-to-end test documentation (not committed)

### 4. **Load Balancing Algorithm**

| Aspect | SQL Server | SFDC |
|--------|------------|------|
| **Default Max Tables** | 1000 per pipeline | 30 per pipeline |
| **Balancing Method** | Count-based (priority vs non-priority) | Prefix+priority explicit grouping |
| **Auto-balancing** | Groups by count | Groups by prefix+priority combo |

---

## Alignment Status

### ✅ Successfully Aligned

1. **Directory structure** matches SQL Server pattern
   - `load_balancing/` folder created
   - `deployment/` folder structure similar
   - `run_pipeline_generation.py` unified runner

2. **Separation of concerns**
   - Load balancing logic in `load_balancing/`
   - YAML generation in `deployment/`
   - Unified workflow in root

3. **Example files**
   - Input examples in `load_balancing/examples/`
   - Output examples in `deployment/examples/`

### 🔧 Items to Clean Up (Optional)

1. **Remove duplicate files:**
   - `README (1).md` (duplicate, can delete)
   - `deployment/auto_balance.py` (legacy, already copied to load_balancing)

2. **Consolidate deployment folder:**
   - Move `resources/sfdc_pipeline.yml` generation to match pattern
   - Clean up old example files if not needed

3. **Update README.md:**
   - Document new prefix+priority logic
   - Update examples to show current structure
   - Add usage instructions matching SQL Server pattern

4. **Add dab/ folder (optional):**
   - Could add utilities similar to SQL Server's `dab/` folder
   - Would contain test utilities and additional tools

---

## Input CSV Format Comparison

### SQL Server Input

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,priority_flag
sales_db,dbo,customers,bronze,sales,customers,0
sales_db,dbo,orders,bronze,sales,orders,1
inventory_db,dbo,products,bronze,inventory,products,0
```

**Key Columns:**
- `source_database`: Groups tables by database (creates separate gateways)
- `priority_flag`: 0=normal, 1=priority (creates separate pipeline groups)

### SFDC Input

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,prefix,priority,connection_name,schedule
Salesforce,standard,Account,main,sfdc_test,Account,business_unit1,01,sfdc-pravin-dabs,*/15 * * * *
Salesforce,standard,Contact,main,sfdc_test,Contact,business_unit1,01,sfdc-pravin-dabs,*/15 * * * *
Salesforce,standard,Opportunity,main,sfdc_test,Opportunity,business_unit1,02,sfdc-pravin-dabs,*/15 * * * *
```

**Key Columns:**
- `prefix`: Business unit or logical grouping (e.g., "business_unit1", "sales_team")
- `priority`: Priority level within prefix (e.g., "01", "02", "03")
- `connection_name`: Salesforce connection in Databricks (SFDC-specific)
- `schedule`: Cron schedule per table (SFDC-specific)

---

## Output Pipeline Naming

### SQL Server

**Format:** `pipeline_group: <number>`

Example:
```
Database: sales_db
  - pipeline_group: 1 (1000 priority tables)
  - pipeline_group: 2 (1000 normal tables)
  - pipeline_group: 3 (500 normal tables)

Database: inventory_db
  - pipeline_group: 4 (800 tables)
```

**Pipeline Name in Databricks:** `my_project_ingestion_1`, `my_project_ingestion_2`, etc.

### SFDC

**Format:** `pipeline_group: <prefix>_<priority>`

Example:
```
Prefix: business_unit1
  - pipeline_group: business_unit1_1 (Account, Contact, Campaign)
  - pipeline_group: business_unit1_2 (Opportunity)

Prefix: business_unit2
  - pipeline_group: business_unit2_1 (Lead, Case)
  - pipeline_group: business_unit2_2 (CustomObject)
```

**Pipeline Name in Databricks:** `SFDC Ingestion - business_unit1_1`, `SFDC Ingestion - business_unit1_2`, etc.

---

## Workflow Comparison

### SQL Server Workflow

```
1. Input CSV with tables
   ↓
2. load_balancing/generate_pipeline_config.py
   → Groups by database + priority
   → Assigns gateway per database
   → Creates pipeline_group (numeric)
   ↓
3. deployment/generate_dab_yaml.py
   → Generates gateways.yml (one per database)
   → Generates pipelines.yml (one per pipeline_group)
   ↓
4. Deploy to Databricks
```

### SFDC Workflow

```
1. Input CSV with prefix + priority
   ↓
2. load_balancing/generate_pipeline_config.py
   → Groups by prefix + priority
   → Creates pipeline_group (prefix_priority)
   ↓
3. deployment/generate_dab_yaml.py
   → Generates sfdc_pipeline.yml (pipelines + jobs)
   ↓
4. Deploy to Databricks
```

**Key Difference:** No gateway generation for SFDC (SaaS connector).

---

## Recommended Next Steps

### High Priority (Alignment)

1. ✅ **DONE:** Create `load_balancing/` folder
2. ✅ **DONE:** Update `generate_pipeline_config.py` with prefix+priority logic
3. ✅ **DONE:** Update `generate_dab_yaml.py` to use new grouping
4. ✅ **DONE:** Update `run_pipeline_generation.py` unified runner
5. ✅ **DONE:** Test end-to-end deployment

### Low Priority (Cleanup)

6. 🔧 **TODO:** Delete `README (1).md` (duplicate)
7. 🔧 **TODO:** Update main `README.md` with new structure
8. 🔧 **TODO:** Remove or archive legacy `deployment/auto_balance.py`
9. 🔧 **TODO:** Consolidate example files

### Optional Enhancements

10. 💡 **OPTIONAL:** Add `dab/` folder with utilities (like SQL Server)
11. 💡 **OPTIONAL:** Add unit tests for grouping logic
12. 💡 **OPTIONAL:** Add CI/CD pipeline for automated testing

---

## Summary

### ✅ Current Status: **ALIGNED**

The SFDC directory structure now matches the SQL Server pattern with:
- ✅ Separate `load_balancing/` folder
- ✅ Separate `deployment/` folder
- ✅ Unified `run_pipeline_generation.py` runner
- ✅ Similar `examples/` structure
- ✅ Prefix+priority grouping implemented
- ✅ End-to-end tested and deployed

### 🎯 Differences are Expected

The remaining differences are **intentional** due to architectural differences:
- SFDC is SaaS (no gateway needed)
- SFDC uses prefix+priority (more flexible than numeric groups)
- SFDC includes Databricks notebook (`main.ipynb`)
- SFDC includes setup utilities (`prework.py`)

**The alignment objective has been successfully achieved!** 🎉

---

*Document created: December 9, 2024*
