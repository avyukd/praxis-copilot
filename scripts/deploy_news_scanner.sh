#!/usr/bin/env bash
# Deploy news scanner Lambda + EventBridge hourly schedule + S3 trigger for digest → dispatch.
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

REGION="${REGION:-us-east-2}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${S3_BUCKET:-praxis-copilot}"

ECR_REPO="${ECR_REPO:-8k-scanner}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"

ROLE_NAME="${ROLE_NAME:-8k-scanner-lambda-role}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

SNS_TOPIC_ARN="${SNS_TOPIC_ARN:?Set SNS_TOPIC_ARN}"
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY}"

NEWS_SCANNER_FUNCTION="${NEWS_SCANNER_FUNCTION:-praxis-news-scanner}"
NEWS_SCANNER_RULE="${NEWS_SCANNER_RULE:-praxis-news-scanner-hourly}"
DISPATCH_FUNCTION="${DISPATCH_FUNCTION:-event-dispatch}"

# Hourly during extended US market hours (UTC: 12-22 covers 7am-5pm ET +/- DST)
NEWS_SCANNER_CRON="${NEWS_SCANNER_CRON:-cron(0 12-22 ? * MON-FRI *)}"

echo "=== Deploying News Scanner ==="
echo "Region: ${REGION}"
echo "Account: ${ACCOUNT_ID}"
echo "Bucket: ${BUCKET}"
echo "Function: ${NEWS_SCANNER_FUNCTION}"

# --- Lambda Function ---

TAVILY_API_KEY="${TAVILY_API_KEY:-}"
NEWS_SCANNER_ENV="{S3_BUCKET=${BUCKET},ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY},SNS_TOPIC_ARN=${SNS_TOPIC_ARN},TAVILY_API_KEY=${TAVILY_API_KEY}}"

echo "--- Lambda: ${NEWS_SCANNER_FUNCTION} ---"
if aws lambda get-function --function-name "${NEWS_SCANNER_FUNCTION}" --region "${REGION}" >/dev/null 2>&1; then
  aws lambda update-function-code \
    --function-name "${NEWS_SCANNER_FUNCTION}" \
    --image-uri "${IMAGE_URI}" \
    --region "${REGION}" >/dev/null

  aws lambda wait function-updated --function-name "${NEWS_SCANNER_FUNCTION}" --region "${REGION}"

  aws lambda update-function-configuration \
    --function-name "${NEWS_SCANNER_FUNCTION}" \
    --image-config "Command=src.modules.events.news_scanner.handler.handler" \
    --environment "Variables=${NEWS_SCANNER_ENV}" \
    --timeout 300 \
    --memory-size 512 \
    --region "${REGION}" >/dev/null
else
  aws lambda create-function \
    --function-name "${NEWS_SCANNER_FUNCTION}" \
    --package-type Image \
    --code "ImageUri=${IMAGE_URI}" \
    --role "${ROLE_ARN}" \
    --image-config "Command=src.modules.events.news_scanner.handler.handler" \
    --environment "Variables=${NEWS_SCANNER_ENV}" \
    --timeout 300 \
    --memory-size 512 \
    --region "${REGION}" >/dev/null
fi

aws lambda wait function-active-v2 --function-name "${NEWS_SCANNER_FUNCTION}" --region "${REGION}"

# --- IAM: allow SSM read for SerpAPI key ---

if ! aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "AllowSSMReadSerpApiKey" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"ssm:GetParameter\"],
        \"Resource\": \"arn:aws:ssm:${REGION}:${ACCOUNT_ID}:parameter/praxis/serpapi_key\"
      }
    ]
  }" >/dev/null; then
  echo "WARN: could not attach SSM read policy"
fi

# --- EventBridge Hourly Schedule ---

echo "--- EventBridge rule: ${NEWS_SCANNER_RULE} ---"
aws events put-rule \
  --name "${NEWS_SCANNER_RULE}" \
  --schedule-expression "${NEWS_SCANNER_CRON}" \
  --state ENABLED \
  --description "Hourly news scanner sweep during market hours" \
  --region "${REGION}" >/dev/null

NEWS_SCANNER_ARN=$(aws lambda get-function --function-name "${NEWS_SCANNER_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)

aws lambda add-permission \
  --function-name "${NEWS_SCANNER_FUNCTION}" \
  --statement-id "eventbridge-news-scanner-cron" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${NEWS_SCANNER_RULE}" \
  --region "${REGION}" >/dev/null 2>&1 || true

aws events put-targets \
  --rule "${NEWS_SCANNER_RULE}" \
  --targets "[{\"Id\":\"news-scanner\",\"Arn\":\"${NEWS_SCANNER_ARN}\"}]" \
  --region "${REGION}" >/dev/null

# --- S3 Notification: news digest → dispatch ---
# We need to add a trigger for data/news/*/digest/*.yaml → dispatch Lambda.
# This requires merging with the existing bucket notification configuration.

echo "--- Adding S3 notification for news digest → dispatch ---"

# Add S3 invoke permission for dispatch (idempotent)
aws lambda add-permission \
  --function-name "${DISPATCH_FUNCTION}" \
  --statement-id "s3-trigger-news-digest" \
  --action "lambda:InvokeFunction" \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::${BUCKET}" \
  --region "${REGION}" >/dev/null 2>&1 || true

echo "=== News Scanner deploy complete ==="
echo "Function: ${NEWS_SCANNER_FUNCTION}"
echo "Schedule: ${NEWS_SCANNER_CRON}"
echo ""
echo "NOTE: S3 notification for news digest → dispatch must be included in"
echo "the main deploy_8k_events.sh S3 notification config to avoid overwriting."
echo "Run deploy_8k_events.sh to apply the full notification configuration."
