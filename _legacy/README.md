# Legacy Files Archive

This folder contains legacy code that is no longer used in the active codebase but preserved for historical reference.

## Files Archived

### SFDC (Salesforce) Connector

#### `sfdc-deployment-auto_balance.py`
- **Original Location:** `sfdc/deployment/auto_balance.py`
- **Date Archived:** 2026-01-09
- **Reason:** Replaced by prefix+priority grouping approach in `sfdc/load_balancing/generate_pipeline_config.py`
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

**Last Updated:** 2026-01-09
