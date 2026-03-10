#!/usr/bin/env bash
# Deploy helper for the manage intraday Lambda + EventBridge rule.
# Runs every 15 minutes during US market hours (9:00-16:00 ET = 13:00-20:00 UTC).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-${REPO_ROOT}/.env.deploy}"

if [[ -f "${DEPLOY_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${DEPLOY_ENV_FILE}"
  set +a
fi

REGION="${REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${S3_BUCKET:-praxis-copilot}"
FUNCTION_NAME="${MANAGE_FUNCTION:-praxis-manage-intraday}"
RULE_NAME="${MANAGE_INTRADAY_RULE:-praxis-manage-intraday-15m}"

echo "=== Configuring Manage Intraday System ==="
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo "Bucket: $BUCKET"
echo "Function: $FUNCTION_NAME"

# Verify Lambda function exists
aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" >/dev/null

FUNC_ARN=$(aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" --query 'Configuration.FunctionArn' --output text)

# Update Lambda code from the shared ECR image
ECR_REPO="${ECR_REPO:-8k-scanner}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"

aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --image-uri "${IMAGE_URI}" \
  --region "$REGION" >/dev/null
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
echo "Updated Lambda code from ${IMAGE_URI}"

# Ensure timeout and env vars are set for price + options flow scan
MANAGE_ENV="{EODHD_API_KEY=${EODHD_API_KEY:?Set EODHD_API_KEY},S3_BUCKET=${BUCKET},SNS_TOPIC_ARN=${SNS_TOPIC_ARN:?Set SNS_TOPIC_ARN}}"
aws lambda update-function-configuration \
  --function-name "$FUNCTION_NAME" \
  --image-config "Command=src.modules.manage.handler.lambda_handler" \
  --environment "Variables=${MANAGE_ENV}" \
  --timeout 120 \
  --memory-size 256 \
  --region "$REGION" >/dev/null
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
echo "Set Lambda timeout to 120s with options flow support"

# EventBridge rule: every 15 minutes, Mon-Fri, 13:00-20:00 UTC (9am-4pm ET)
aws events put-rule \
  --name "$RULE_NAME" \
  --schedule-expression "cron(*/15 13-20 ? * MON-FRI *)" \
  --state ENABLED \
  --description "Intraday manage scan every 15 minutes during market hours" \
  --region "$REGION" >/dev/null

aws events put-targets \
  --rule "$RULE_NAME" \
  --targets "[{\"Id\":\"manage-intraday\",\"Arn\":\"${FUNC_ARN}\",\"Input\":\"{\\\"mode\\\":\\\"intraday\\\"}\"}]" \
  --region "$REGION" >/dev/null

aws lambda add-permission \
  --function-name "$FUNCTION_NAME" \
  --statement-id "eventbridge-${RULE_NAME}" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
  --region "$REGION" >/dev/null 2>&1 || true

# Sync manage config to S3
aws s3 cp "${REPO_ROOT}/config/manage.yaml" "s3://${BUCKET}/config/manage.yaml" \
  --region "$REGION" >/dev/null
echo "Synced manage.yaml to s3://${BUCKET}/config/manage.yaml"

echo "=== Manage intraday configuration complete ==="
