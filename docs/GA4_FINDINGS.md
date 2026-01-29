# GA4 Lakeflow Connect - Testing Findings

## Summary

Tested both **table-level** and **schema-level** ingestion approaches for GA4 Lakeflow Connect. Results confirm GA4 requires schema-level ingestion only.

## Test Results

### 1. Schema-Level Ingestion (CORRECT APPROACH)

**Configuration:**
```csv
source_catalog,source_schema,source_table_name
<GCP_PROJECT_ID>,analytics_999999999,
```
(Empty `source_table_name` triggers schema-level ingestion)

**Generated YAML:**
```yaml
objects:
- schema:
    source_catalog: <GCP_PROJECT_ID>
    source_schema: analytics_999999999
    destination_catalog: ${var.dest_catalog}
    destination_schema: ${var.dest_schema}
```

**Result:**
- Pipeline state: `COMPLETED`
- Tables ingested: **0 tables**
- Reason: Source dataset `analytics_999999999` is empty

**Working Example:**
- Pipeline ID: `128b1698-b8c3-409b-aab0-63c13ecc7baf` (test_ga4)
- Connection: `g4a_demo`
- Source: `fe-dev-sandbox.analytics_2222222`
- Destination: `ga4_demo_hyunsung.log`
- Tables ingested: **1 table** (`events`)
- Status: ✅ **WORKING**

### 2. Table-Level Ingestion (NOT SUPPORTED)

**Configuration:**
```csv
source_catalog,source_schema,source_table_name
<GCP_PROJECT_ID>,analytics_999999999,events_20210131
```

**Generated YAML:**
```yaml
objects:
- table:
    source_catalog: <GCP_PROJECT_ID>
    source_schema: analytics_999999999
    source_table: events_20210131
    destination_catalog: ${var.dest_catalog}
    destination_schema: ${var.dest_schema}
    destination_table: events_20210131
```

**Result:**
- Pipeline state: `FAILED`
- Error: `SAAS_PARTIAL_ANALYSIS_INPUT_CREATION_ERROR`
- Error message: "Error encountered while creating input for partial analysis"

## Key Findings

### 1. GA4 Requires Schema-Level Ingestion Only

GA4 Lakeflow Connect **does not support table-level ingestion**. This is by design because:
- GA4 BigQuery exports create date-partitioned tables (`events_20210101`, `events_20210102`, etc.)
- Manually specifying each table would require hundreds of entries
- Schema-level ingestion automatically discovers all tables including new date partitions

**Reference:** README_GA4.md lines 278-359

### 2. Naming Convention Requirement

BigQuery datasets must follow GA4 naming convention:
- Required format: `analytics_[property_id]`
- Examples: `analytics_123456789`, `analytics_2222222`, `analytics_999999999`
- Custom names like `ga4_demo_data` will fail with `SCHEMA_NOT_FOUND`

### 3. Our Pipeline Status

**Current Configuration:**
- Connection: `ga4_demo_connection` ✅
- Source: `<GCP_PROJECT_ID>.analytics_999999999` ✅
- Naming convention: Correct ✅
- Ingestion type: Schema-level ✅
- Service account permissions: Verified ✅

**Problem:**
- Dataset `analytics_999999999` exists but is **empty**
- `bq` command was not available to copy tables from `ga4_demo_data`
- Pipeline completes successfully but ingests nothing (expected behavior for empty dataset)

## Architecture Comparison

### Working Pipeline (test_ga4)
```
fe-dev-sandbox.analytics_2222222 (has data)
    ↓ (connection: g4a_demo)
    ↓ (schema-level ingestion)
    ↓
ga4_demo_hyunsung.log
    ✅ events table (with data)
```

### Our Pipeline (ga4_ingestion_pipeline_group_1)
```
<GCP_PROJECT_ID>.analytics_999999999 (empty)
    ↓ (connection: ga4_demo_connection)
    ↓ (schema-level ingestion)
    ↓
ga4_connector.ga4
    ⚠️  No tables (source is empty)
```

## Next Steps

To get data ingestion working, need to:

1. **Option A: Populate analytics_999999999 with data**
   - Copy tables from existing GA4 dataset
   - Requires `bq` CLI tool or BigQuery API access
   - Commands:
     ```bash
     bq cp source_project:source_dataset.table_name \
           <GCP_PROJECT_ID>:analytics_999999999.table_name
     ```

2. **Option B: Use existing populated dataset**
   - Change source in CSV to point to dataset with actual GA4 data
   - Ensure dataset follows `analytics_*` naming convention
   - Update `ga4_demo_connection` credentials if needed

3. **Option C: Use public GA4 demo dataset**
   - Source: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`
   - Already contains GA4 data
   - Would need different connection (public BigQuery)

## Validated Configuration

The following configuration is **correct** for GA4 Lakeflow Connect:

**CSV (ga4_config_demo.csv):**
```csv
source_database,source_catalog,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,connection_name,schedule
GA4,<GCP_PROJECT_ID>,analytics_999999999,,ga4_connector,ga4,,1,ga4_demo_connection,0 */6 * * *
```

**Key Points:**
- ✅ `source_table_name` is **empty** (schema-level ingestion)
- ✅ `source_schema` follows `analytics_*` pattern
- ✅ `connection_name` points to user's connection
- ✅ Generator creates correct schema-level YAML

**Generated YAML:**
```yaml
variables:
  dest_catalog:
    default: ga4_connector
  dest_schema:
    default: ga4
resources:
  pipelines:
    pipeline_ga4_ingestion_group_1:
      name: ga4_ingestion_pipeline_group_1
      catalog: ${var.dest_catalog}
      ingestion_definition:
        connection_name: ga4_demo_connection
        objects:
        - schema:
            source_catalog: <GCP_PROJECT_ID>
            source_schema: analytics_999999999
            destination_catalog: ${var.dest_catalog}
            destination_schema: ${var.dest_schema}
```

## Conclusion

Infrastructure is **correctly configured**:
- ✅ Connection authentication working
- ✅ Pipeline configuration follows GA4 requirements
- ✅ Schema-level ingestion (not table-level)
- ✅ Naming conventions correct
- ✅ DAB deployment successful

**Remaining issue:** Source dataset is empty. Once populated with GA4 data, ingestion will work as demonstrated by the working `test_ga4` pipeline.
