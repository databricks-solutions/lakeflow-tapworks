# Google Analytics 4 Connector

Automated pipeline generation for GA4 to Databricks ingestion using Lakeflow Connect with BigQuery integration.

## Quick Start

### Option 1: Interactive Notebook (Recommended)

1. Upload `pipeline_setup.ipynb` to your Databricks workspace
2. Configure parameters in the notebook widgets
3. Run all cells to generate and verify pipeline configuration
4. Deploy using Databricks Asset Bundles

### Option 2: Python Script

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create Google BigQuery connection in Databricks (with service account)

# 3. Prepare your input CSV (see examples/business_units/pipeline_config.csv)

# 4. Run the generator
python pipeline_generator.py

# 5. Deploy
cd examples/business_units/deployment
databricks bundle deploy -t prod
```

## Input CSV Format

Your CSV should include these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_catalog` | Yes | GCP project ID | `my-gcp-project` |
| `source_schema` | Yes | GA4 property ID | `analytics_123456789` |
| `tables` | Yes | Tables to ingest | `"events,events_intraday,users"` |
| `target_catalog` | Yes | Target catalog | `ga4_catalog` |
| `target_schema` | Yes | Target schema | `google_analytics` |
| `prefix` | Yes | Business unit/grouping | `marketing`, `sales` |
| `priority` | Yes | Priority level | `01`, `02`, `03` |
| `connection_name` | No | Connection name | `ga4_connection` |
| `schedule` | No | Cron schedule | `0 */6 * * *` |

**Example CSV:**
```csv
source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,connection_name,schedule
my-gcp-project,analytics_123456789,"events,events_intraday,users",ga4_catalog,google_analytics,marketing,01,ga4_connection,0 */6 * * *
my-gcp-project,analytics_987654321,"events,events_intraday",ga4_catalog,google_analytics,marketing,01,ga4_connection,0 */6 * * *
my-gcp-project,analytics_111222333,events,ga4_catalog,google_analytics,sales,02,ga4_connection,0 */12 * * *
```

## Key Features

- **BigQuery Integration** - Ingests GA4 data via BigQuery export
- **Multiple Properties** - Supports multiple GA4 properties in single configuration
- **Flexible Grouping** - Organize by business unit (prefix) and priority
- **Per-Pipeline Schedules** - Configure individual schedules via CSV
- **Table Selection** - Choose which GA4 tables to ingest (events, events_intraday, users)

## Pipeline Grouping

Pipelines are created based on `(prefix, priority)` combinations:

```
prefix=marketing, priority=01  → Pipeline: marketing_01
prefix=marketing, priority=02  → Pipeline: marketing_02
prefix=sales, priority=01      → Pipeline: sales_01
```

All GA4 properties with the same prefix+priority are grouped into one pipeline.

## Generated Output

The tool generates a Databricks Asset Bundle structure:

```
examples/{example_name}/deployment/
├── databricks.yml           # Bundle configuration
└── resources/
    ├── ga4_pipeline.yml     # Pipeline definitions
    └── jobs.yml             # Scheduled jobs
```

## Prerequisites

- Databricks workspace with Unity Catalog
- **Google BigQuery connection** in Databricks with service account key
- GA4 BigQuery export enabled for your properties
- GCP service account with BigQuery Data Viewer role
- Databricks CLI installed and authenticated
- Python 3.8+ with required packages

### Creating BigQuery Connection

1. Go to Databricks workspace → **Catalog → Connections**
2. Click **"Create Connection"**
3. Select **Google BigQuery** as connection type
4. Enter connection name (e.g., `ga4_connection`)
5. Upload service account JSON key file
6. Verify connection status is "Active"

## Configuration

The `pipeline_generator.py` script accepts these parameters:

- `input_csv` - Path to input CSV file
- `default_connection_name` - Default connection name (default: "ga4_connection")
- `default_schedule` - Default cron schedule (default: "0 */6 * * *")
- `output_yaml` - Path for generated YAML file
- `output_config` - Path for generated configuration CSV

## GA4 Tables

The connector supports three GA4 BigQuery export tables:

- **events** - Daily events table (partitioned by date)
- **events_intraday** - Real-time intraday events (updated throughout the day)
- **users** - User-level data aggregations

Specify tables as comma-separated values in the `tables` column.

## Examples

See the `examples/` directory for sample configurations:

- `examples/business_units/` - Multiple properties grouped by business unit

## Deployment

```bash
cd examples/{example_name}/deployment
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

Jobs are created in **PAUSED** state. To enable scheduling:

```bash
# List jobs
databricks jobs list | grep ga4_pipeline

# Unpause job
databricks jobs update <job_id> --schedule-pause-status UNPAUSED
```

## Project Structure

```
ga4/
├── README.md
├── requirements.txt
├── load_balancing/
│   ├── load_balancer.py           # Pipeline grouping logic
│   └── examples/
│       └── ga4_config.csv         # Example input
├── deployment/
│   └── connector_settings_generator.py  # YAML generation
├── examples/
│   └── business_units/            # Example
│       ├── pipeline_config.csv
│       └── deployment/
│           ├── databricks.yml
│           └── resources/
│               ├── ga4_pipeline.yml
│               └── jobs.yml
├── pipeline_generator.py          # Unified workflow
└── pipeline_setup.ipynb           # Interactive notebook
```

## Support

For detailed information, see the comprehensive documentation in `_legacy/README.md`.
