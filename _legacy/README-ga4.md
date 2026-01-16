# Google Analytics 4 Lakeflow Connect

Databricks Asset Bundle (DAB) for ingesting Google Analytics 4 data via Lakeflow Connect.

## Overview

This connector enables automated ingestion of GA4 data (events, events_intraday, users) into Databricks Delta tables using Lakeflow Connect with BigQuery integration.

**Architecture:**
```
GA4 → BigQuery → Connection (Service Account) → Ingestion Pipeline → Delta Tables
```

## Quick Start

### 1. Prerequisites

- Databricks workspace with Unity Catalog
- GCP service account with BigQuery Data Viewer role
- GA4 BigQuery export enabled
- Databricks CLI installed and authenticated

### 2. Create GA4 Connection

Create a Google BigQuery connection in Databricks:

```bash
# Via UI: Catalog → Connections → Create Connection → Google BigQuery
# Upload service account JSON key
# Connection name: ga4_connection
```

### 3. Configure Input CSV

Create a CSV with GA4 properties to ingest:

```csv
source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,schedule
my-gcp-project,analytics_123456789,"events,events_intraday,users",ga4_connector,google_analytics,business_unit1,01,0 */6 * * *
my-gcp-project,analytics_987654321,"events,events_intraday",ga4_connector,google_analytics,business_unit1,01,0 */6 * * *
my-gcp-project,analytics_111222333,events,ga4_connector,google_analytics,business_unit2,01,0 */12 * * *
```

**Columns:**
- `source_catalog`: GCP project ID
- `source_schema`: GA4 property ID (e.g., analytics_123456789)
- `tables`: Comma-separated list (events, events_intraday, users)
- `target_catalog`: Destination catalog
- `target_schema`: Destination schema
- `prefix`: Business unit or logical grouping
- `priority`: Priority level (01, 02, 03)
- `schedule`: Cron schedule

### 4. Generate Pipeline Configuration

```bash
python run_pipeline_generation.py \
  --csv input_config.csv \
  --output-yaml deployment/resources/ga4_pipeline.yml \
  --output-config pipeline_config.csv \
  --schedule "0 */6 * * *"
```

This creates:
- Pipeline configuration CSV with `pipeline_group` column
- Databricks Asset Bundle YAML with pipelines and scheduled jobs

### 5. Deploy

```bash
cd deployment
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Pipeline Grouping

Each unique `(prefix, priority)` combination creates a separate pipeline:

**Example:**
```csv
prefix,priority → pipeline_group
business_unit1,01 → business_unit1_01
business_unit1,02 → business_unit1_02
business_unit2,01 → business_unit2_01
```

**Use cases:**
- Group by business unit (marketing, sales, analytics)
- Separate by priority (critical, standard, low)
- Different schedules per group

## Project Structure

```
ga4/
├── load_balancing/
│   ├── generate_pipeline_config.py    # Prefix+priority grouping logic
│   └── examples/
│       └── example_config.csv         # Input CSV template
├── deployment/
│   ├── generate_dab_yaml.py           # YAML generator
│   ├── main.ipynb                     # Databricks notebook
│   └── resources/
│       └── ga4_pipeline.yml           # Generated YAML (git ignored)
├── run_pipeline_generation.py         # Unified CLI runner
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## Configuration Reference

### CSV Columns

| Column | Description | Example |
|--------|-------------|---------|
| `source_catalog` | GCP project ID | `my-gcp-project` |
| `source_schema` | GA4 property ID | `analytics_123456789` |
| `tables` | Tables to ingest | `"events,events_intraday,users"` |
| `target_catalog` | Destination catalog | `ga4_connector` |
| `target_schema` | Destination schema | `google_analytics` |
| `prefix` | Business unit grouping | `business_unit1`, `marketing` |
| `priority` | Priority level | `01`, `02`, `03` |
| `schedule` | Cron schedule | `0 */6 * * *` (every 6 hours) |

### Schedule Format

Standard cron (5 fields) is auto-converted to Quartz cron (6 fields):

```
0 */6 * * *     →  0 0 */6 * * ?     (every 6 hours)
0 */12 * * *    →  0 0 */12 * * ?    (every 12 hours)
0 0 * * *       →  0 0 0 * * ?       (daily at midnight)
```

## Deployment Modes

### CSV Mode (Recommended)
Use CSV with prefix+priority for explicit pipeline grouping:

```bash
python run_pipeline_generation.py \
  --csv input.csv \
  --output-yaml resources/ga4_pipeline.yml
```

### Auto-Discover Mode
Auto-discover GA4 properties from BigQuery with bin-packing:

```bash
python run_pipeline_generation.py \
  --auto-discover \
  --pipelines 3 \
  --output-yaml resources/ga4_pipeline.yml
```

## Using the Databricks Notebook

1. Upload `deployment/main.ipynb` to Databricks workspace
2. Update paths to your catalog/schema/volume
3. Run cells step-by-step to generate pipelines
4. Uses Spark for processing (no pandas required)

## Monitoring

### Check Pipeline Status

```bash
# List pipelines
databricks pipelines list | grep "GA4 Ingestion"

# View pipeline details
databricks pipelines get --pipeline-id <pipeline_id>
```

### Query Data

```sql
-- Check ingested tables
SHOW TABLES IN ga4_connector.google_analytics;

-- Query GA4 events
SELECT * FROM ga4_connector.google_analytics.analytics_123456789_events
WHERE event_date >= '20240101'
LIMIT 10;
```

## Troubleshooting

### Connection Not Found

Verify connection exists:
```bash
databricks connections get ga4_connection
```

### BigQuery Authentication Failed

Check service account permissions:
- BigQuery Data Viewer role
- Access to GA4 datasets

### Pipeline Not Starting

1. Verify connection is active
2. Check catalog/schema exist
3. Grant connection permissions
4. Review pipeline event logs

## Additional Resources

- [Databricks Lakeflow Connect](https://docs.databricks.com/en/ingestion/index.html)
- [GA4 BigQuery Export](https://support.google.com/analytics/answer/9358801)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
