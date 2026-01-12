# Connector Architecture Overview

**Version:** 1.0
**Last Updated:** 2026-01-09
**Purpose:** Overview of connector architecture and types for Lakehouse Tapworks

---

## Table of Contents

1. [Overview](#overview)
2. [Connector Types](#connector-types)
   - [Database Connectors](#1-database-connectors)
   - [SaaS Connectors](#2-saas-connectors)

---

## Overview

This document provides an architectural overview of connectors in the Lakehouse Tapworks framework, explaining the core patterns and connector types.

### Architecture

All connectors follow a two-step process:

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: Load Balancing (load_balancing/)                       │
│  • Groups tables/objects into pipeline configurations          │
│  • Generates pipeline_group assignments                         │
│  • Outputs CSV with grouping metadata                           │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: YAML Generation (deployment/)                          │
│  • Reads pipeline configuration from Step 1                     │
│  • Generates Databricks Asset Bundle YAML files                │
│  • Creates pipelines, jobs, and (optionally) gateways          │
└─────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Modularity** - Load balancing and YAML generation are separate, reusable components
2. **Consistency** - Same patterns across all connector types
3. **Flexibility** - Support connector-specific requirements while maintaining standards
4. **Testability** - Each component can be tested independently

---

## Connector Types

### 1. Database Connectors

**Examples:** SQL Server, MySQL, PostgreSQL, Oracle

**Characteristics:**
- ✅ Require gateway for connectivity (on-premises or VPC)
- ✅ Support multiple source databases
- ✅ Typically 1000s of tables
- ✅ Need automatic load balancing
- ✅ Use connection ID from Databricks API
- ✅ Support CDC (Change Data Capture)

**Output Files:**
- `gateways.yml` - Gateway resource definitions
- `pipelines.yml` - Pipeline resource definitions
- `databricks.yml` - Root DAB configuration

**Key Features:**
- Gateway per source database
- Automatic table grouping (e.g., max 1000 tables per pipeline)
- Priority table separation
- Cluster configuration (node types, worker count)

---

### 2. SaaS Connectors

**Examples:** Salesforce (SFDC), Google Analytics 4 (GA4), ServiceNow, Workday

**Characteristics:**
- ❌ No gateway required (direct OAuth connection)
- ✅ Single source system
- ✅ Typically 100-500 objects/tables
- ✅ Manual grouping via prefix+priority
- ✅ Connection name hardcoded or from CSV
- ✅ Supports column-level filtering

**Output Files:**
- `{connector}_pipeline.yml` - Combined pipeline and job definitions
- `databricks.yml` - Root DAB configuration (optional)

**Key Features:**
- Explicit user-defined grouping (prefix + priority)
- Column filtering (include/exclude)
- Schema-level ingestion support (optional)
- No cluster configuration needed

---

**For implementation details, see:** [Connector Development Guide](CONNECTOR_DEVELOPMENT_GUIDE.md)
