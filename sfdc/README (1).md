# Lakehouse Tapworks - Databricks Lakeflow Connect

Multi-source data ingestion framework for Databricks using Lakeflow Connect and Databricks Asset Bundles (DABs).

## Overview

This project provides infrastructure-as-code generators for ingesting data from multiple sources into Databricks using:
- **Lakeflow Connect** - Serverless ingestion with CDC support
- **Databricks Asset Bundles (DABs)** - Infrastructure-as-code for pipeline deployment
- **Unity Catalog** - Centralized governance and connections

## Available Connectors

### Salesforce
Ingest Salesforce objects (Account, Contact, Opportunity, etc.) with OAuth-based connection.

**Architecture:** Salesforce (SaaS) → Connection (OAuth) → Ingestion Pipeline → Delta Tables

📁 [View Salesforce Documentation](sfdc/README.md)

**Features:**
- CSV-driven pipeline configuration
- Column-level filtering (include/exclude)
- Multiple pipeline groups
- Example configurations and generated YAML

---

### Google Analytics 4 (GA4)
Ingest GA4 event data and analytics metrics.

📁 [View GA4 Documentation](ga4/README.md)

**Features:**
- Event stream ingestion
- Custom dimensions and metrics
- Automated schema evolution

---

### PostgreSQL
Ingest PostgreSQL tables with CDC support via gateways.

📁 [View PostgreSQL Documentation](postgres/README.md)

**Features:**
- On-premises connectivity via gateways
- CDC (Change Data Capture) support
- Batch and incremental ingestion
- Auto-balancing across gateways

---

## Quick Start

1. **Choose your connector** and navigate to its directory
2. **Follow the connector-specific README** for detailed setup instructions
3. **Configure your connection** in Databricks Unity Catalog
4. **Generate pipeline YAML** using the provided generator scripts
5. **Deploy with DABs** using `databricks bundle deploy`

## Prerequisites

### Databricks Workspace
- Unity Catalog enabled
- Permissions to create connections, catalogs, and pipelines

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Configure Databricks CLI
databricks configure
```

## Project Structure

```
lakehouse-tapworks/
├── README.md                    # This file
├── requirements.txt             # Common Python dependencies
├── docs/                        # Shared documentation
├── sfdc/                        # Salesforce connector
│   ├── README.md               # Salesforce setup guide
│   └── dab/                    # DAB generator and templates
├── ga4/                         # Google Analytics 4 connector
│   ├── README.md               # GA4 setup guide
│   └── dab/                    # DAB generator and templates
└── postgres/                    # PostgreSQL connector
    ├── README.md               # PostgreSQL setup guide
    └── dab/                    # DAB generator and templates
```

## Common Patterns

### Connection Management
All connectors use Unity Catalog connections:

```bash
# List connections
databricks connections list

# Get connection details
databricks connections get <connection_name>
```

### Deployment Workflow
1. Configure tables in CSV
2. Generate YAML: `python dab/generate_*_pipeline.py config.csv`
3. Validate: `databricks bundle validate -t dev`
4. Deploy: `databricks bundle deploy -t dev`
5. Start pipeline: `databricks pipelines start-update <pipeline_id>`

### Multiple Environments
All connectors support dev/staging/prod targets:

```bash
# Development
databricks bundle deploy -t dev

# Production
databricks bundle deploy -t prod
```

## Architecture Patterns

### SaaS Connectors (Salesforce, GA4)
```
Source (SaaS) → Connection (OAuth) → Lakeflow Pipeline → Delta Tables
```
- No gateway required
- Direct API access
- OAuth authentication

### On-Premises Connectors (PostgreSQL, SQL Server)
```
Source (On-Prem) → Gateway → Connection → Lakeflow Pipeline → Delta Tables
```
- Gateway required for connectivity
- CDC support via gateway
- Multiple gateways for load balancing

## Resources

- [Databricks Lakeflow Connect](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)

## Contributing

1. Fork the repository
2. Create a feature branch for your connector/changes
3. Test with your Databricks workspace
4. Submit a pull request

## Support

For connector-specific questions, refer to the README in each connector directory.
