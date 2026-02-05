# ServiceNow Connector Examples

This directory contains example configurations for the ServiceNow connector.

## Available Examples

### basic/
Basic ServiceNow ingestion configuration demonstrating:
- Multiple business unit groupings (business_unit1, business_unit2, business_unit3)
- Different subgroup levels (01, 02)
- Various schedules (*/15, */30, hourly)
- Column filtering (include_columns and exclude_columns)
- SCD type configuration (SCD_TYPE_1 and SCD_TYPE_2)
- Common ServiceNow tables (incident, sys_user, cmdb_ci_server, etc.)

## Usage

### Generate DAB from Example

```bash
# From the servicenow directory
python pipeline_generator.py \
  --input-csv examples/basic/pipeline_config.csv \
  --project-name servicenow_ingestion \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir deployment
```

### Customize for Your Environment

1. Copy an example CSV:
   ```bash
   cp examples/basic/pipeline_config.csv my_config.csv
   ```

2. Edit `my_config.csv`:
   - Update `connection_name` to match your ServiceNow connection
   - Update `target_catalog` and `target_schema` as needed
   - Modify `source_table_name` to match your required tables
   - Adjust `schedule`, `prefix`, and `subgroup` as needed
   - Configure column filtering and SCD types

3. Generate your pipelines:
   ```bash
   python pipeline_generator.py \
     --input-csv my_config.csv \
     --project-name my_servicenow_project \
     --workspace-host https://your-workspace.cloud.databricks.com
   ```

## Example Features Demonstrated

### Business Unit Grouping
```csv
prefix,subgroup
business_unit1,01  # High subgroup tables for BU1
business_unit1,02  # Lower subgroup tables for BU1
business_unit2,01  # High subgroup tables for BU2
```

### Schedule Variation
```csv
schedule
*/15 * * * *  # Every 15 minutes
*/30 * * * *  # Every 30 minutes
0 * * * *     # Every hour
```

### Column Filtering
```csv
include_columns,exclude_columns
"sys_id,sys_updated_on,name"  # Only these columns
,"user_password,last_password"  # All except these columns
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
