# Workday Reports Connector Examples

This directory contains example configurations for the Workday Reports connector.

## Available Examples

### basic/
Basic Workday Reports ingestion configuration demonstrating:
- Multiple department groupings (hr, recruiting, finance)
- Different subgroup levels (01, 02)
- Various schedules (*/6, */12, hourly, daily, weekly)
- Column filtering (include_columns and exclude_columns)
- SCD type configuration (SCD_TYPE_1 and SCD_TYPE_2)
- **Primary key configuration (REQUIRED for Workday Reports)**
- Common Workday reports (employees, compensation, requisitions, etc.)

## Usage

### Generate DAB from Example

```bash
# From the workday_reports directory
python pipeline_generator.py \
  --input-csv examples/basic/pipeline_config.csv \
  --project-name workday_reports_ingestion \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir deployment
```

### Customize for Your Environment

1. Copy an example CSV:
   ```bash
   cp examples/basic/pipeline_config.csv my_config.csv
   ```

2. Edit `my_config.csv`:
   - Update `source_url` to match your Workday report URLs
   - Update `connection_name` to match your Workday connection
   - Update `target_catalog` and `target_schema` as needed
   - **REQUIRED**: Set `primary_keys` for each report (e.g., "Employee_ID")
   - Adjust `schedule`, `prefix`, and `subgroup` as needed
   - Configure column filtering and SCD types

3. Generate your pipelines:
   ```bash
   python pipeline_generator.py \
     --input-csv my_config.csv \
     --project-name my_workday_project \
     --workspace-host https://your-workspace.cloud.databricks.com
   ```

## Example Features Demonstrated

### Department Grouping
```csv
prefix,subgroup
hr,01           # High subgroup HR reports
hr,02           # Lower subgroup HR reports
recruiting,01   # Recruiting reports
```

### Schedule Variation
```csv
schedule
0 */6 * * *   # Every 6 hours
0 */12 * * *  # Every 12 hours
0 0 * * *     # Daily at midnight
```

### Primary Key Configuration (REQUIRED)
```csv
primary_keys
Employee_ID                      # Single primary key
"Worker_ID,Effective_Date"       # Composite primary key
```

### Column Filtering
```csv
include_columns,exclude_columns
"Employee_ID,Worker_ID,Legal_Name,Hire_Date"  # Only these columns
,"Salary_Amount,SSN,Medical_History"           # All except these columns
```

### SCD Type Configuration
```csv
scd_type
SCD_TYPE_2  # Track history (default)
SCD_TYPE_1  # Overwrite (no history)
```

## Next Steps

After generating your DAB project:

1. **Validate**: Review the generated YAML files
2. **Deploy**: Use `databricks bundle deploy`
3. **Monitor**: Check pipeline status in Databricks UI
4. **Optimize**: Adjust schedules and groupings based on performance
