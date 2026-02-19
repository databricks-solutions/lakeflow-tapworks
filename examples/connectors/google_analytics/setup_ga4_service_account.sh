#!/usr/bin/env bash
#
# Setup Google Cloud Service Account for GA4 BigQuery Export
#
# This script:
# 1. Creates a GCP service account
# 2. Grants necessary IAM permissions for GA4 BigQuery access
# 3. Creates and downloads service account key JSON
# 4. Stores credentials in Databricks secrets
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - GCP project with GA4 BigQuery export enabled
#   - Databricks CLI installed and authenticated
#
# Usage:
#   ./setup_ga4_service_account.sh <gcp_project_id> <databricks_secret_scope>
#
# Example:
#   ./setup_ga4_service_account.sh my-gcp-project ga4_secrets

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "GA4 SERVICE ACCOUNT SETUP"
echo "========================================================================"

# Validate arguments
if [ $# -lt 2 ]; then
    echo -e "${RED}❌ Error: Missing required arguments${NC}"
    echo ""
    echo "Usage:"
    echo "  $0 <gcp_project_id> <databricks_secret_scope>"
    echo ""
    echo "Example:"
    echo "  $0 my-gcp-project ga4_secrets"
    exit 1
fi

GCP_PROJECT_ID="$1"
SECRET_SCOPE="$2"
SERVICE_ACCOUNT_NAME="databricks-ga4-ingestion"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="/tmp/${SERVICE_ACCOUNT_NAME}-key.json"

echo ""
echo "Configuration:"
echo "  GCP Project: $GCP_PROJECT_ID"
echo "  Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "  Databricks Secret Scope: $SECRET_SCOPE"
echo ""

# Check prerequisites
echo "1. Checking prerequisites..."
if ! command -v gcloud &> /dev/null; then
    echo -e "   ${RED}❌ gcloud CLI not found${NC}"
    echo "   Install: https://cloud.google.com/sdk/docs/install"
    exit 1
fi
echo -e "   ${GREEN}✅ gcloud CLI found${NC}"

if ! command -v databricks &> /dev/null; then
    echo -e "   ${RED}❌ databricks CLI not found${NC}"
    echo "   Install: pip install databricks-cli"
    exit 1
fi
echo -e "   ${GREEN}✅ databricks CLI found${NC}"

# Set GCP project
echo ""
echo "2. Setting GCP project..."
gcloud config set project "$GCP_PROJECT_ID"
echo -e "   ${GREEN}✅ Project set${NC}"

# Create service account
echo ""
echo "3. Creating service account..."
if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" &> /dev/null; then
    echo -e "   ${YELLOW}⚠️  Service account already exists${NC}"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="Databricks GA4 Ingestion" \
        --description="Service account for Databricks Lakeflow Connect to access GA4 BigQuery export"
    echo -e "   ${GREEN}✅ Service account created${NC}"
fi

# Grant BigQuery permissions
echo ""
echo "4. Granting BigQuery permissions..."
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.dataViewer" \
    --condition=None \
    > /dev/null
echo -e "   ${GREEN}✅ BigQuery Data Viewer role granted${NC}"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.jobUser" \
    --condition=None \
    > /dev/null
echo -e "   ${GREEN}✅ BigQuery Job User role granted${NC}"

# Create and download key
echo ""
echo "5. Creating service account key..."
# Delete existing key file if present
rm -f "$KEY_FILE"

gcloud iam service-accounts keys create "$KEY_FILE" \
    --iam-account="$SERVICE_ACCOUNT_EMAIL"
echo -e "   ${GREEN}✅ Key created: $KEY_FILE${NC}"

# Validate JSON
if ! jq empty "$KEY_FILE" 2>/dev/null; then
    echo -e "   ${RED}❌ Generated key is not valid JSON${NC}"
    exit 1
fi
echo -e "   ${GREEN}✅ Key validated${NC}"

# Create Databricks secret scope
echo ""
echo "6. Creating Databricks secret scope..."
if databricks secrets create-scope "$SECRET_SCOPE" 2>/dev/null; then
    echo -e "   ${GREEN}✅ Secret scope created${NC}"
else
    echo -e "   ${YELLOW}⚠️  Secret scope may already exist (continuing)${NC}"
fi

# Store service account JSON in secrets
echo ""
echo "7. Storing service account in Databricks secrets..."
SERVICE_ACCOUNT_JSON=$(cat "$KEY_FILE")
databricks secrets put-secret "$SECRET_SCOPE" service_account_json \
    --string-value "$SERVICE_ACCOUNT_JSON"
echo -e "   ${GREEN}✅ Service account stored in secrets${NC}"

# Cleanup
echo ""
echo "8. Cleaning up local key file..."
rm -f "$KEY_FILE"
echo -e "   ${GREEN}✅ Local key file deleted${NC}"

# Display summary
echo ""
echo "========================================================================"
echo -e "${GREEN}✅ SETUP COMPLETE!${NC}"
echo "========================================================================"
echo ""
echo "Service Account Created:"
echo "  Email: $SERVICE_ACCOUNT_EMAIL"
echo "  Roles: BigQuery Data Viewer, BigQuery Job User"
echo ""
echo "Credentials Stored:"
echo "  Databricks Secret Scope: $SECRET_SCOPE"
echo "  Secret Key: service_account_json"
echo ""
echo "Next Steps:"
echo "  1. Create GA4 connection:"
echo "     python create_ga4_connection.py my_ga4_connection $SECRET_SCOPE"
echo ""
echo "  2. Configure tables in ga4_config.csv"
echo ""
echo "  3. Generate pipeline YAML:"
echo "     python generate_ga4_pipeline.py ga4_config.csv"
echo ""
echo "  4. Deploy:"
echo "     cd ga4_dab && databricks bundle deploy -t dev"
echo ""
