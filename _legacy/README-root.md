# Databricks Lakeflow Connect Pipeline Generator

This toolkit provides a solution for generating load-balanced Databricks Lakeflow Connect pipelines using user defined configurations.

## Overview

The toolkit automatically generates Databricks Asset Bundle YAMLs needed to deploy the piplines based user defined configurations as well as intelligent table grouping and resource optimization. 
Use cases include:
- **Extract list from the source:** Extract a list of existing tables from sources like SFDC, SQL Server or folder structure and use that information to automatically create DAB
- **Migaration:** Continue leveraging exsiting meta data used by an existing data ingestion tools 
- **Automated naming:** e.g., specify pipeline names based on some conventions or table mapping based on some logic 


## Current Connetors
Database connectors
SQL Server

SaaS Connectors
Salesforce


### Components

1. **Load Balancing** (`load_balancing/generate_pipeline_config.py`) - Groups tables into optimized pipeline configurations
2. **YAML Generation** (`deployment/generate_dab_yaml.py`) - Creates Databricks Asset Bundle YAML files
3. **Unified Runner** (`run_pipeline_generation.py`) - Combines both steps into a single workflow


## Architecture

There are two major steps (each can be run indpeendently)

- Load balancing: based on some logic assigns the tables to different pipelines and geenrates the configuration for the second step (table mapping based on some logic can happen here)
- DAB YAML geenration: using the config geenrated in the first step or manually generate DAB YAMLs for the specific connector
   

### Two-Part Process

```
┌─────────────────────────────────────────────────────────────────┐
│                      INPUT: Source Table List                   │
│  (source_database, source_schema, source_table_name, etc.)      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                    PART 1: Load Balancing                                      │
│  • Groups tables into pipeline_groups                                          │
│  • Use source table list and pipeline groups to genrate pipeline configuration │
└────────────────────────────┬───────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 OUTPUT: Pipeline Configuration                  │
│     (all input columns + pipeline_group + gateway + ...)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PART 2: YAML Generation                       │
│  • Generates Databricks Asset Bundle resources                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OUTPUT: YAML Files                          │
│              (gateways.yml + pipelines.yml)                     │
└─────────────────────────────────────────────────────────────────┘
```

