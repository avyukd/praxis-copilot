#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-${REPO_ROOT}/.env.deploy}"

if [[ -f "${DEPLOY_ENV_FILE}" ]]; then
  echo "Loading deploy env: ${DEPLOY_ENV_FILE}"
  set -a
  # shellcheck disable=SC1090
  source "${DEPLOY_ENV_FILE}"
  set +a
fi

REGION="${REGION:-us-east-2}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

ECR_REPO="${ECR_REPO:-8k-scanner}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"

ROLE_NAME="${ROLE_NAME:-8k-scanner-lambda-role}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

S3_BUCKET="${S3_BUCKET:-praxis-copilot}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:?Set SNS_TOPIC_ARN}"

SEC_USER_AGENT="${SEC_USER_AGENT:?Set SEC_USER_AGENT}"
FMP_API_KEY="${FMP_API_KEY:-}"
EODHD_API_KEY="${EODHD_API_KEY:-}"
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY}"
DISABLE_LLM_ANALYSIS="${DISABLE_LLM_ANALYSIS:-0}"
MARKET_CAP_THRESHOLD="${MARKET_CAP_THRESHOLD:-500000000}"
CA_MARKET_CAP_THRESHOLD="${CA_MARKET_CAP_THRESHOLD:-500000000}"

SCANNER_CRON="${SCANNER_CRON:-cron(* 11-23,0-2 ? * MON-FRI *)}"

POLLER_FUNCTION="${POLLER_FUNCTION:-8k-scanner-poller}"
EXTRACTOR_FUNCTION="${EXTRACTOR_FUNCTION:-8k-scanner-extractor}"
ANALYZER_FUNCTION="${ANALYZER_FUNCTION:-8k-scanner-analyzer}"
CA_POLLER_FUNCTION="${CA_POLLER_FUNCTION:-8k-scanner-ca-poller}"
CA_ANALYZER_FUNCTION="${CA_ANALYZER_FUNCTION:-8k-scanner-ca-analyzer}"
US_GNW_POLLER_FUNCTION="${US_GNW_POLLER_FUNCTION:-8k-scanner-us-gnw-poller}"
US_GNW_ANALYZER_FUNCTION="${US_GNW_ANALYZER_FUNCTION:-8k-scanner-us-gnw-analyzer}"
DISPATCH_FUNCTION="${DISPATCH_FUNCTION:-8k-scanner-dispatch}"

POLLER_RULE="${POLLER_RULE:-8k-scanner-poller-cron}"
CA_POLLER_RULE="${CA_POLLER_RULE:-8k-scanner-ca-poller-cron}"
US_GNW_POLLER_RULE="${US_GNW_POLLER_RULE:-8k-scanner-us-gnw-poller-cron}"

echo "=== Account: ${ACCOUNT_ID}, Region: ${REGION}, Bucket: ${S3_BUCKET} ==="

echo "--- Sync config/ to s3://${S3_BUCKET}/config/ ---"
aws s3 sync "${REPO_ROOT}/config" "s3://${S3_BUCKET}/config" --region "${REGION}" --delete

echo "--- ECR repo ---"
aws ecr describe-repositories --repository-names "${ECR_REPO}" --region "${REGION}" >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name "${ECR_REPO}" --region "${REGION}" >/dev/null

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "--- Build and push image (${IMAGE_URI}) ---"
docker buildx build --platform linux/amd64 --provenance=false -t "${IMAGE_URI}" --push "${REPO_ROOT}"

echo "--- IAM role ---"
if ! aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }' >/dev/null

  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/AmazonSNSFullAccess
  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/AWSLambdaRole

  echo "Waiting for IAM role propagation..."
  sleep 10
fi

COMMON_VARS="S3_BUCKET=${S3_BUCKET},SEC_USER_AGENT=${SEC_USER_AGENT},FMP_API_KEY=${FMP_API_KEY},EODHD_API_KEY=${EODHD_API_KEY},MARKET_CAP_THRESHOLD=${MARKET_CAP_THRESHOLD},CA_MARKET_CAP_THRESHOLD=${CA_MARKET_CAP_THRESHOLD}"
POLLER_ENV="{${COMMON_VARS}}"
EXTRACTOR_ENV="{${COMMON_VARS}}"
ANALYZER_ENV="{${COMMON_VARS},SNS_TOPIC_ARN=${SNS_TOPIC_ARN},ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY},DISABLE_LLM_ANALYSIS=${DISABLE_LLM_ANALYSIS}}"
CA_POLLER_ENV="{${COMMON_VARS}}"
CA_ANALYZER_ENV="{${COMMON_VARS},SNS_TOPIC_ARN=${SNS_TOPIC_ARN},ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY},DISABLE_LLM_ANALYSIS=${DISABLE_LLM_ANALYSIS}}"
US_GNW_POLLER_ENV="{${COMMON_VARS}}"
US_GNW_ANALYZER_ENV="{${COMMON_VARS},SNS_TOPIC_ARN=${SNS_TOPIC_ARN},ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY},DISABLE_LLM_ANALYSIS=${DISABLE_LLM_ANALYSIS}}"
DISPATCH_ENV="{S3_BUCKET=${S3_BUCKET}}"

create_or_update_lambda() {
  local func_name="$1"
  local handler="$2"
  local env_vars="$3"

  echo "--- Lambda: ${func_name} (${handler}) ---"
  if aws lambda get-function --function-name "${func_name}" --region "${REGION}" >/dev/null 2>&1; then
    aws lambda update-function-code \
      --function-name "${func_name}" \
      --image-uri "${IMAGE_URI}" \
      --region "${REGION}" >/dev/null

    aws lambda wait function-updated --function-name "${func_name}" --region "${REGION}"

    aws lambda update-function-configuration \
      --function-name "${func_name}" \
      --image-config "Command=${handler}" \
      --environment "Variables=${env_vars}" \
      --timeout 300 \
      --memory-size 512 \
      --region "${REGION}" >/dev/null
  else
    aws lambda create-function \
      --function-name "${func_name}" \
      --package-type Image \
      --code "ImageUri=${IMAGE_URI}" \
      --role "${ROLE_ARN}" \
      --image-config "Command=${handler}" \
      --environment "Variables=${env_vars}" \
      --timeout 300 \
      --memory-size 512 \
      --region "${REGION}" >/dev/null
  fi

  aws lambda wait function-active-v2 --function-name "${func_name}" --region "${REGION}"
}

create_or_update_lambda "${POLLER_FUNCTION}" "src.modules.events.eight_k_scanner.poller_handler.lambda_handler" "${POLLER_ENV}"
create_or_update_lambda "${EXTRACTOR_FUNCTION}" "src.modules.events.eight_k_scanner.extractor_handler.lambda_handler" "${EXTRACTOR_ENV}"
create_or_update_lambda "${ANALYZER_FUNCTION}" "src.modules.events.eight_k_scanner.analyzer_handler.lambda_handler" "${ANALYZER_ENV}"
create_or_update_lambda "${CA_POLLER_FUNCTION}" "src.modules.events.eight_k_scanner.ca_handler.lambda_handler" "${CA_POLLER_ENV}"
create_or_update_lambda "${CA_ANALYZER_FUNCTION}" "src.modules.events.eight_k_scanner.ca_analyzer_handler.lambda_handler" "${CA_ANALYZER_ENV}"
create_or_update_lambda "${US_GNW_POLLER_FUNCTION}" "src.modules.events.eight_k_scanner.us_gnw_handler.lambda_handler" "${US_GNW_POLLER_ENV}"
create_or_update_lambda "${US_GNW_ANALYZER_FUNCTION}" "src.modules.events.eight_k_scanner.us_gnw_analyzer_handler.lambda_handler" "${US_GNW_ANALYZER_ENV}"
create_or_update_lambda "${DISPATCH_FUNCTION}" "src.modules.events.dispatch.handler.lambda_handler" "${DISPATCH_ENV}"

ensure_rule_target() {
  local rule_name="$1"
  local func_name="$2"
  local statement_id="$3"

  aws events put-rule \
    --name "${rule_name}" \
    --schedule-expression "${SCANNER_CRON}" \
    --state ENABLED \
    --region "${REGION}" >/dev/null

  local func_arn
  func_arn=$(aws lambda get-function --function-name "${func_name}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)

  aws lambda add-permission \
    --function-name "${func_name}" \
    --statement-id "${statement_id}" \
    --action "lambda:InvokeFunction" \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${rule_name}" \
    --region "${REGION}" >/dev/null 2>&1 || true

  aws events put-targets \
    --rule "${rule_name}" \
    --targets "Id=${func_name},Arn=${func_arn}" \
    --region "${REGION}" >/dev/null
}

echo "--- EventBridge cron rules ---"
ensure_rule_target "${POLLER_RULE}" "${POLLER_FUNCTION}" "eventbridge-cron"
ensure_rule_target "${CA_POLLER_RULE}" "${CA_POLLER_FUNCTION}" "eventbridge-cron-ca"
ensure_rule_target "${US_GNW_POLLER_RULE}" "${US_GNW_POLLER_FUNCTION}" "eventbridge-cron-us-gnw"

extractor_arn=$(aws lambda get-function --function-name "${EXTRACTOR_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
analyzer_arn=$(aws lambda get-function --function-name "${ANALYZER_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
ca_analyzer_arn=$(aws lambda get-function --function-name "${CA_ANALYZER_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
us_gnw_analyzer_arn=$(aws lambda get-function --function-name "${US_GNW_ANALYZER_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
dispatch_arn=$(aws lambda get-function --function-name "${DISPATCH_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)

add_s3_permission() {
  local func_name="$1"
  local statement_id="$2"
  aws lambda add-permission \
    --function-name "${func_name}" \
    --statement-id "${statement_id}" \
    --action "lambda:InvokeFunction" \
    --principal s3.amazonaws.com \
    --source-arn "arn:aws:s3:::${S3_BUCKET}" \
    --region "${REGION}" >/dev/null 2>&1 || true
}

add_s3_permission "${EXTRACTOR_FUNCTION}" "s3-trigger-extractor"
add_s3_permission "${ANALYZER_FUNCTION}" "s3-trigger-analyzer"
add_s3_permission "${CA_ANALYZER_FUNCTION}" "s3-trigger-ca-analyzer"
add_s3_permission "${US_GNW_ANALYZER_FUNCTION}" "s3-trigger-us-gnw-analyzer"
add_s3_permission "${DISPATCH_FUNCTION}" "s3-trigger-dispatch"

echo "--- S3 notification wiring (${S3_BUCKET}) ---"
aws s3api put-bucket-notification-configuration \
  --bucket "${S3_BUCKET}" \
  --region "${REGION}" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"Id\": \"8k-extractor-index\",
        \"LambdaFunctionArn\": \"${extractor_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/8k/\"},
          {\"Name\": \"Suffix\", \"Value\": \"index.json\"}
        ]}}
      },
      {
        \"Id\": \"8k-analyzer-extracted\",
        \"LambdaFunctionArn\": \"${analyzer_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/8k/\"},
          {\"Name\": \"Suffix\", \"Value\": \"extracted.json\"}
        ]}}
      },
      {
        \"Id\": \"ca-analyzer-index\",
        \"LambdaFunctionArn\": \"${ca_analyzer_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/ca-pr/\"},
          {\"Name\": \"Suffix\", \"Value\": \"index.json\"}
        ]}}
      },
      {
        \"Id\": \"us-gnw-analyzer-index\",
        \"LambdaFunctionArn\": \"${us_gnw_analyzer_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/us-pr/\"},
          {\"Name\": \"Suffix\", \"Value\": \"index.json\"}
        ]}}
      },
      {
        \"Id\": \"dispatch-8k-analysis\",
        \"LambdaFunctionArn\": \"${dispatch_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/8k/\"},
          {\"Name\": \"Suffix\", \"Value\": \"analysis.json\"}
        ]}}
      },
      {
        \"Id\": \"dispatch-ca-analysis\",
        \"LambdaFunctionArn\": \"${dispatch_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/ca-pr/\"},
          {\"Name\": \"Suffix\", \"Value\": \"analysis.json\"}
        ]}}
      },
      {
        \"Id\": \"dispatch-us-analysis\",
        \"LambdaFunctionArn\": \"${dispatch_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/us-pr/\"},
          {\"Name\": \"Suffix\", \"Value\": \"analysis.json\"}
        ]}}
      }
    ]
  }"

echo "=== Deploy complete ==="
echo "Next: run scripts/release_smoke_check.sh"
