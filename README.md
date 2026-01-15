# Lakehouse Tapworks

Automated pipeline generation toolkit for Databricks Lakeflow Connect ingestion.

## Overview

Lakehouse Tapworks simplifies creating and managing Databricks Delta Live Tables (DLT) ingestion pipelines using Databricks Asset Bundles (DAB). The toolkit automatically generates optimized pipeline configurations from simple CSV inputs, handling load balancing, scheduling, and resource allocation.

## Supported Connectors

### Database Connectors
- **SQL Server** - On-premises and cloud SQL Server databases with gateway support

### SaaS Connectors
- **Salesforce** - Salesforce objects via OAuth connection
- **Google Analytics 4** - GA4 data via BigQuery integration

## Key Features

- **Automated Load Balancing** - Intelligently groups tables into optimized pipeline configurations
- **Per-Pipeline Scheduling** - Configure individual schedules for each pipeline via CSV
- **Infrastructure as Code** - Generates Databricks Asset Bundle (DAB) YAML files ready for deployment
- **Unified Architecture** - Consistent structure and workflow across all connectors
- **Interactive Notebooks** - Databricks notebooks for interactive pipeline configuration

## Quick Start

Each connector provides three usage options:

1. **Interactive Notebook** (`pipeline_setup.ipynb`) - Run directly in Databricks workspace with visual feedback
2. **Python Script** (`pipeline_generator.py`) - Complete workflow in a single command
3. **Programmatic** - Import and use functions in your own scripts

### Basic Workflow

```bash
# 1. Navigate to connector directory
cd sqlserver  # or sfdc, ga4

# 2. Install dependencies
pip install -r requirements.txt

# 3. Prepare input CSV with tables/objects to ingest

# 4. Run pipeline generator
python pipeline_generator.py

# 5. Deploy to Databricks
cd examples/your_example/deployment
databricks bundle deploy -t prod
```

## Project Structure

```
lakehouse-tapworks/
├── sqlserver/          # SQL Server connector
├── sfdc/               # Salesforce connector
├── ga4/                # Google Analytics 4 connector
├── CONNECTOR_DEVELOPMENT_GUIDE.md  # Guide for adding new connectors
└── README.md           # This file
```

Each connector follows a unified structure:

```
{connector}/
├── README.md                           # Connector-specific documentation
├── load_balancing/
│   └── load_balancer.py               # Load balancing logic
├── deployment/
│   └── connector_settings_generator.py # DAB YAML generation
├── examples/
│   └── {example_name}/
│       ├── pipeline_config.csv        # Input configuration
│       └── deployment/                # Generated DAB files
├── pipeline_generator.py              # Unified workflow script
└── pipeline_setup.ipynb               # Interactive notebook
```

## Documentation

- **Connector-Specific Guides**: See README.md in each connector directory (sqlserver/, sfdc/, ga4/)
- **Development Guide**: See [CONNECTOR_DEVELOPMENT_GUIDE.md](CONNECTOR_DEVELOPMENT_GUIDE.md) for implementing new connectors
- **Legacy Documentation**: Historical documentation available in [_legacy/](_legacy/)

## Architecture

The toolkit uses a two-stage process:

```
Input CSV → Load Balancing → Pipeline Config → YAML Generation → DAB Files
```

1. **Load Balancing** - Groups tables/objects into optimized pipeline configurations based on:
   - Source database isolation (database connectors)
   - Priority flags for time-sensitive data
   - Maximum tables per pipeline
   - User-defined grouping (SaaS connectors)

2. **YAML Generation** - Creates Databricks Asset Bundle files including:
   - Gateway configurations (database connectors)
   - Pipeline definitions
   - Job schedules
   - Resource configurations

## Requirements

- Python 3.8+
- Databricks workspace with Unity Catalog
- Databricks CLI configured
- Appropriate source system credentials

## Getting Started

1. **Choose a connector** based on your data source (sqlserver, sfdc, or ga4)
2. **Read the connector README** in the specific connector directory
3. **Prepare your input CSV** following the connector's format
4. **Run the pipeline generator** using the interactive notebook or Python script
5. **Deploy to Databricks** using the generated DAB files

## Contributing

To add a new connector, follow the [CONNECTOR_DEVELOPMENT_GUIDE.md](CONNECTOR_DEVELOPMENT_GUIDE.md).

## Support

For issues, questions, or contributions, please open an issue in the repository.
