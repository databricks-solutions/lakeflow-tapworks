# Legacy Files Archive

This folder contains legacy code that is no longer used in the active codebase but preserved for historical reference.

## Files Archived

### Documentation

#### `README-root.md`
- **Original Location:** Root `/README.md`
- **Date Archived:** 2026-01-16
- **Reason:** Replaced with simplified, user-friendly README
- **What It Contained:**
  - Detailed technical architecture documentation
  - Component descriptions (load_balancing, deployment, unified runner)
  - Legacy file naming (generate_pipeline_config.py, generate_dab_yaml.py, run_pipeline_generation.py)
  - Deployment instructions and troubleshooting details
- **Why Replaced:**
  - New README focuses on quick start and connector overview
  - Detailed technical documentation moved to connector-specific READMEs
  - Updated to reflect renamed files (load_balancer.py, connector_settings_generator.py, pipeline_generator.py)
  - Streamlined for better user experience

#### `README-sqlserver.md`
- **Original Location:** `sqlserver/README.md`
- **Date Archived:** 2026-01-16
- **Reason:** Replaced with simplified README
- **What It Contained:**
  - Comprehensive SQL Server connector documentation
  - Detailed API reference and configuration parameters
  - Extensive troubleshooting section
  - Multiple deployment workflows and best practices
  - Legacy file naming conventions
- **Why Replaced:**
  - Updated to reflect renamed modules
  - Simplified structure focusing on quick start
  - Removed deployment-specific details (moved to per-example documentation)
  - Streamlined configuration section

#### `README-sfdc.md`
- **Original Location:** `salesforce/README.md` (formerly sfdc/)
- **Date Archived:** 2026-01-16
- **Reason:** Replaced with simplified README
- **What It Contained:**
  - Comprehensive Salesforce connector documentation
  - Detailed DAB configuration examples
  - Step-by-step connection setup instructions
  - Monitoring and troubleshooting guides
- **Why Replaced:**
  - Updated to reflect renamed modules
  - Simplified for easier adoption
  - Focus on essential quick start steps
  - Removed verbose deployment instructions

#### `README-salesforce-extra.md`
- **Original Location:** `salesforce/README (1).md`
- **Date Archived:** 2026-01-16
- **Reason:** Duplicate README file
- **What It Contained:**
  - Appears to be a duplicate or alternative version of Salesforce documentation
- **Why Archived:**
  - Redundant with main Salesforce README
  - Likely created accidentally during file operations

#### `README-ga4.md`
- **Original Location:** `google_analytics/README.md` (formerly ga4/)
- **Date Archived:** 2026-01-16
- **Reason:** Replaced with simplified README
- **What It Contained:**
  - Comprehensive GA4 connector documentation
  - Auto-discovery mode documentation
  - Databricks notebook usage details
  - BigQuery integration setup
- **Why Replaced:**
  - Updated to reflect renamed modules
  - Simplified structure matching other connectors
  - Focus on CSV-based workflow (primary use case)
  - Streamlined BigQuery connection setup

### SFDC (Salesforce) Connector

#### `sfdc-deployment-auto_balance.py`
- **Original Location:** `salesforce/deployment/auto_balance.py` (formerly sfdc/)
- **Date Archived:** 2026-01-09
- **Reason:** Replaced by prefix+priority grouping approach in `salesforce/load_balancing/load_balancer.py`
- **What It Did:**
  - Automatic bin-packing load balancer for Salesforce objects
  - Connected to Salesforce API to query row counts
  - Used First Fit Decreasing (FFD) algorithm to distribute tables across pipelines
  - Provided automatic balancing based on data size
- **Why Removed:**
  - SFDC connector now uses explicit user-defined grouping (prefix + priority)
  - Automatic bin-packing not needed for SaaS connectors with fewer objects (~100-500)
  - User control over grouping provides better business logic alignment
  - No imports or references found in active codebase
- **Dependencies:** `simple-salesforce`, `python-dotenv`

---

## Archive Policy

Files are moved here when:
1. **Not actively used** - No imports or references in active code
2. **Replaced by newer implementation** - Superseded by better approach
3. **Historical value** - May be useful for reference or revival

Files should be renamed to show their original path:
- Format: `{connector}-{path}-{filename}.{ext}`
- Example: `sfdc-deployment-auto_balance.py` (from `sfdc/deployment/auto_balance.py`)

## Restoration

To restore a legacy file:
1. Copy file from `_legacy/` back to original location
2. Rename to remove path prefix
3. Update imports/references as needed
4. Test thoroughly
5. Remove from this archive

---

**Last Updated:** 2026-01-16
