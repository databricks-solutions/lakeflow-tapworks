# Salesforce YAML Generator

A flexible script to generate Databricks Asset Bundle (DAB) YAML configurations for Salesforce Lakeflow Connect ingestion pipelines.

## Features

-  **No hardcoding**: Works with any Salesforce connection
-  **Multi-pipeline support**: Groups tables using `pipeline_group` column
-  **Serverless by default**: Generates cost-efficient serverless pipelines
-  **Flexible paths**: Auto-detects output directory or accepts custom path
-  **Validation**: Checks for configuration inconsistencies

## Usage

```bash
python generate_sfdc_yaml_simple.py <csv_path> [output_dir]
```

### Examples

**Basic usage** (auto-detects output directory):
```bash
python generate_sfdc_yaml_simple.py salesforce_sample_config.csv
```

**Custom output directory**:
```bash
python generate_sfdc_yaml_simple.py /path/to/config.csv /path/to/output
```

**Absolute path**:
```bash
python generate_sfdc_yaml_simple.py /Users/john/projects/sfdc/config.csv
```

## CSV Configuration Format

Required columns:
- `source_database`: Source system (e.g., "Salesforce")
- `source_schema`: Source schema (e.g., "standard")
- `source_table_name`: Salesforce object name (e.g., "Account")
- `target_catalog`: Databricks catalog (e.g., "bronze")
- `target_schema`: Databricks schema (e.g., "salesforce")
- `target_table_name`: Target table name (usually same as source)
- `pipeline_group`: Group number (1, 2, 3, etc.)
- `gateway`: Gateway ID (not used for Salesforce, can be any value)
- `connection_name`: Databricks connection name
- `schedule`: Cron schedule (not yet implemented)

### Example CSV

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,gateway,connection_name,schedule
Salesforce,standard,Account,bronze,salesforce,Account,1,1,my_sfdc_connection,*/15 * * * *
Salesforce,standard,Contact,bronze,salesforce,Contact,1,1,my_sfdc_connection,*/15 * * * *
Salesforce,standard,Opportunity,bronze,salesforce,Opportunity,2,1,my_sfdc_connection,*/15 * * * *
Salesforce,standard,Lead,bronze,salesforce,Lead,2,1,my_sfdc_connection,*/15 * * * *
```

This configuration creates:
- **2 pipelines**: Group 1 (Account, Contact) and Group 2 (Opportunity, Lead)
- **Connection**: Uses existing `my_sfdc_connection`
- **Target**: `bronze.salesforce.*` tables

## Pipeline Groups

The `pipeline_group` column determines how tables are distributed across pipelines:

| Group Value | Result |
|-------------|--------|
| All rows = 1 | Single pipeline with all tables |
| 1, 2, 3 | Three pipelines (one per group) |
| 1, 1, 2, 2, 3 | Three pipelines with different table counts |

**Benefits of multiple pipelines:**
- Parallel ingestion (faster overall)
- Isolated failures (one group fails, others continue)
- Resource management (distribute load)
- Different schedules per group (future enhancement)

## Output Structure

```
salesforce_dab/
├── databricks.yml           # Main DAB configuration
└── resources/
    └── sfdc_pipeline.yml    # Generated pipeline definitions
```

## Generated Pipeline Configuration

The script generates **serverless ingestion pipelines**:

```yaml
resources:
  pipelines:
    pipeline_sfdc_ingestion_group_1:
      name: sfdc_ingestion_pipeline_group_1
      catalog: ${var.dest_catalog}
      ingestion_definition:              # ← Serverless!
        connection_name: my_sfdc_connection
        objects:
          - table:
              source_schema: objects
              source_table: Account
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
```

**Key characteristics:**
-  No `clusters` configuration = serverless
-  Uses `ingestion_definition` (Lakeflow Connect)
-  Pay-per-use pricing (~$1-2 per run)
-  Auto-scales based on workload

## Deployment Workflow

1. **Create Salesforce connection** (if not exists):
   ```bash
   # In Databricks UI:
   # Catalog → External Data → Connections → Create connection
   # Name: my_sfdc_connection
   # Type: Salesforce
   # Add credentials
   ```

2. **Generate YAML**:
   ```bash
   python generate_sfdc_yaml_simple.py salesforce_config.csv
   ```

3. **Review output**:
   ```bash
   cat salesforce_dab/resources/sfdc_pipeline.yml
   ```

4. **Deploy with DABs**:
   ```bash
   cd salesforce_dab
   databricks bundle validate -t dev
   databricks bundle deploy -t dev --auto-approve
   ```

5. **Start pipeline**:
   ```bash
   databricks pipelines start-update <pipeline_id>
   ```

## Multiple Salesforce Connections

To manage multiple Salesforce environments:

**Option 1: Separate CSV files**
```bash
python generate_sfdc_yaml_simple.py prod_salesforce.csv prod_dab/resources
python generate_sfdc_yaml_simple.py dev_salesforce.csv dev_dab/resources
```

**Option 2: Different connection names in same CSV**
- Not recommended: Script uses connection_name from first row
- Better: Create separate CSV files per connection

## Validation

The script validates:
-  CSV file exists and is readable
-  All rows use same `connection_name`
-  All rows use same `target_catalog`
-  All rows use same `target_schema`

Warnings are shown for inconsistencies, but generation continues using first row values.

## Troubleshooting

### Error: "File not found"
```bash
# Use absolute path or run from correct directory
python generate_sfdc_yaml_simple.py /full/path/to/config.csv
```

### Output directory wrong
```bash
# Specify custom output directory
python generate_sfdc_yaml_simple.py config.csv /path/to/salesforce_dab/resources
```

### Connection not found during deployment
```bash
# Verify connection exists in Databricks UI
# Catalog → External Data → Connections
# Name must match exactly (case-sensitive)
```

## Architecture

```
CSV Configuration → Python Script → YAML Files → DABs Deployment → Serverless Pipelines

salesforce_config.csv
        ↓
generate_sfdc_yaml_simple.py
        ↓
salesforce_dab/resources/sfdc_pipeline.yml
        ↓
databricks bundle deploy
        ↓
Salesforce API → Serverless Pipeline → Delta Tables (bronze.salesforce.*)
```

## Comparison: Serverless vs Gateway Pipelines

| Feature | Serverless (Salesforce) | Gateway (SQL Server) |
|---------|------------------------|----------------------|
| Configuration | `ingestion_definition` | `gateway_definition` + `clusters` |
| Runtime | On-demand | 24/7 continuous |
| Cost | ~$1-2 per run | ~$20-50/day |
| Use case | SaaS connectors | Database connectors |
| Scaling | Auto-scales | Fixed cluster size |

Salesforce always uses serverless architecture (no gateway needed).

## Dependencies

- Python 3.7+
- PyYAML: `pip install pyyaml`

## License

Internal Databricks tool for customer engagements.
