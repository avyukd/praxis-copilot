#!/usr/bin/env bash
# Monitor deploy helper for cutover architecture.
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
EVALUATOR_FUNCTION="${MONITOR_EVALUATOR_FUNCTION:-praxis-monitor-evaluator}"
RULE_NAME="${MONITOR_DAILY_RULE:-praxis-monitor-daily}"

echo "=== Configuring Monitor System ==="
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo "Bucket: $BUCKET"
echo "Evaluator: $EVALUATOR_FUNCTION"

aws lambda get-function --function-name "$EVALUATOR_FUNCTION" --region "$REGION" >/dev/null

FUNC_ARN=$(aws lambda get-function --function-name "$EVALUATOR_FUNCTION" --region "$REGION" --query 'Configuration.FunctionArn' --output text)

aws events put-rule \
  --name "$RULE_NAME" \
  --schedule-expression "cron(0 14 ? * MON-FRI *)" \
  --state ENABLED \
  --description "Daily monitor evaluation (search + scraper monitors)" \
  --region "$REGION" >/dev/null

aws events put-targets \
  --rule "$RULE_NAME" \
  --targets "[{\"Id\":\"monitor-evaluator\",\"Arn\":\"${FUNC_ARN}\",\"Input\":\"{\\\"trigger_type\\\":\\\"scheduled\\\"}\"}]" \
  --region "$REGION" >/dev/null

aws lambda add-permission \
  --function-name "$EVALUATOR_FUNCTION" \
  --statement-id "eventbridge-daily-${RULE_NAME}" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
  --region "$REGION" >/dev/null 2>&1 || true

if [ -d "${REPO_ROOT}/config/monitors" ] && ls "${REPO_ROOT}/config/monitors"/*.yaml >/dev/null 2>&1; then
  aws s3 sync "${REPO_ROOT}/config/monitors/" "s3://${BUCKET}/config/monitors/" \
    --region "$REGION" >/dev/null
  echo "Synced monitor configs to s3://${BUCKET}/config/monitors/"
fi

if [ -d "${REPO_ROOT}/config/scrapers" ] && ls "${REPO_ROOT}/config/scrapers"/*.py >/dev/null 2>&1; then
  aws s3 sync "${REPO_ROOT}/config/scrapers/" "s3://${BUCKET}/config/scrapers/" \
    --region "$REGION" >/dev/null
  echo "Synced scraper scripts to s3://${BUCKET}/config/scrapers/"
fi

echo "=== Monitor configuration complete ==="
