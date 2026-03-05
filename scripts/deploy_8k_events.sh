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
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY}"
FMP_API_KEY="${FMP_API_KEY:-}"
EODHD_API_KEY="${EODHD_API_KEY:-}"
DISABLE_LLM_ANALYSIS="${DISABLE_LLM_ANALYSIS:-0}"
MARKET_CAP_THRESHOLD="${MARKET_CAP_THRESHOLD:-500000000}"
CA_MARKET_CAP_THRESHOLD="${CA_MARKET_CAP_THRESHOLD:-500000000}"
FILING_ANALYZER_ENABLED_FORMS="${FILING_ANALYZER_ENABLED_FORMS:-8-K;8-K/A}"
ENABLE_8K_HAIKU_SCREEN="${ENABLE_8K_HAIKU_SCREEN:-1}"
HAIKU_PRESCREEN_MODEL="${HAIKU_PRESCREEN_MODEL:-anthropic/claude-3-haiku-20240307}"

SCANNER_CRON="${SCANNER_CRON:-cron(* 11-23,0-2 ? * MON-FRI *)}"

SEC_POLLER_FUNCTION="${SEC_POLLER_FUNCTION:-sec-filings-poller}"
PRESS_POLLER_FUNCTION="${PRESS_POLLER_FUNCTION:-press-releases-poller}"
EXTRACTOR_FUNCTION="${EXTRACTOR_FUNCTION:-filings-extractor}"
ANALYZER_FUNCTION="${ANALYZER_FUNCTION:-filing-analyzer}"
ALERTS_FUNCTION="${ALERTS_FUNCTION:-filing-alerts}"
DISPATCH_FUNCTION="${DISPATCH_FUNCTION:-event-dispatch}"
MONITOR_EVALUATOR_FUNCTION="${MONITOR_EVALUATOR_FUNCTION:-praxis-monitor-evaluator}"

SEC_POLLER_RULE="${SEC_POLLER_RULE:-sec-filings-poller-cron}"
PRESS_POLLER_RULE="${PRESS_POLLER_RULE:-press-releases-poller-cron}"

LEGACY_FUNCTIONS=(
  "8k-scanner-poller"
  "8k-scanner-extractor"
  "8k-scanner-analyzer"
  "8k-scanner-ca-poller"
  "8k-scanner-ca-analyzer"
  "8k-scanner-us-gnw-poller"
  "8k-scanner-us-gnw-analyzer"
  "8k-scanner-dispatch"
)

LEGACY_RULES=(
  "8k-scanner-poller-cron"
  "8k-scanner-ca-poller-cron"
  "8k-scanner-us-gnw-poller-cron"
)

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

if ! aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "AllowInvokeMonitorEvaluator" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": \"lambda:InvokeFunction\",
        \"Resource\": \"arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${MONITOR_EVALUATOR_FUNCTION}\"
      }
    ]
  }" >/dev/null; then
  echo "WARN: could not attach IAM inline invoke policy to ${ROLE_NAME}."
fi

COMMON_VARS="S3_BUCKET=${S3_BUCKET},SEC_USER_AGENT=${SEC_USER_AGENT},FMP_API_KEY=${FMP_API_KEY},EODHD_API_KEY=${EODHD_API_KEY},MARKET_CAP_THRESHOLD=${MARKET_CAP_THRESHOLD},CA_MARKET_CAP_THRESHOLD=${CA_MARKET_CAP_THRESHOLD}"
SEC_POLLER_ENV="{${COMMON_VARS}}"
PRESS_POLLER_ENV="{${COMMON_VARS}}"
EXTRACTOR_ENV="{${COMMON_VARS}}"
ANALYZER_ENV="{${COMMON_VARS},ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY},DISABLE_LLM_ANALYSIS=${DISABLE_LLM_ANALYSIS},FILING_ANALYZER_ENABLED_FORMS=${FILING_ANALYZER_ENABLED_FORMS},ENABLE_8K_HAIKU_SCREEN=${ENABLE_8K_HAIKU_SCREEN},HAIKU_PRESCREEN_MODEL=${HAIKU_PRESCREEN_MODEL}}"
ALERTS_ENV="{${COMMON_VARS},SNS_TOPIC_ARN=${SNS_TOPIC_ARN},FILING_ANALYZER_ENABLED_FORMS=${FILING_ANALYZER_ENABLED_FORMS}}"
DISPATCH_ENV="{S3_BUCKET=${S3_BUCKET},MONITOR_EVALUATOR_LAMBDA=${MONITOR_EVALUATOR_FUNCTION},FILING_ANALYZER_LAMBDA=${ANALYZER_FUNCTION}}"

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

create_or_update_lambda "${SEC_POLLER_FUNCTION}" "src.modules.events.eight_k_scanner.poller_handler.lambda_handler" "${SEC_POLLER_ENV}"
create_or_update_lambda "${PRESS_POLLER_FUNCTION}" "src.modules.events.eight_k_scanner.press_releases_poller_handler.lambda_handler" "${PRESS_POLLER_ENV}"
create_or_update_lambda "${EXTRACTOR_FUNCTION}" "src.modules.events.eight_k_scanner.extractor_handler.lambda_handler" "${EXTRACTOR_ENV}"
create_or_update_lambda "${ANALYZER_FUNCTION}" "src.modules.events.eight_k_scanner.filing_analyzer_handler.lambda_handler" "${ANALYZER_ENV}"
create_or_update_lambda "${ALERTS_FUNCTION}" "src.modules.events.eight_k_scanner.filing_alerts_handler.lambda_handler" "${ALERTS_ENV}"
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
ensure_rule_target "${SEC_POLLER_RULE}" "${SEC_POLLER_FUNCTION}" "eventbridge-sec-filings-cron"
ensure_rule_target "${PRESS_POLLER_RULE}" "${PRESS_POLLER_FUNCTION}" "eventbridge-press-releases-cron"

extractor_arn=$(aws lambda get-function --function-name "${EXTRACTOR_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
analyzer_arn=$(aws lambda get-function --function-name "${ANALYZER_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
alerts_arn=$(aws lambda get-function --function-name "${ALERTS_FUNCTION}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)
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

add_s3_permission "${EXTRACTOR_FUNCTION}" "s3-trigger-filings-extractor"
add_s3_permission "${ANALYZER_FUNCTION}" "s3-trigger-filing-analyzer"
add_s3_permission "${ALERTS_FUNCTION}" "s3-trigger-filing-alerts"
add_s3_permission "${DISPATCH_FUNCTION}" "s3-trigger-event-dispatch"

echo "--- S3 notification wiring (${S3_BUCKET}) ---"
aws s3api put-bucket-notification-configuration \
  --bucket "${S3_BUCKET}" \
  --region "${REGION}" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"Id\": \"filings-extractor-index\",
        \"LambdaFunctionArn\": \"${extractor_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/filings/\"},
          {\"Name\": \"Suffix\", \"Value\": \"index.json\"}
        ]}}
      },
      {
        \"Id\": \"press-releases-extractor-index\",
        \"LambdaFunctionArn\": \"${extractor_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/press_releases/\"},
          {\"Name\": \"Suffix\", \"Value\": \"index.json\"}
        ]}}
      },
      {
        \"Id\": \"filing-alerts-analysis\",
        \"LambdaFunctionArn\": \"${alerts_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/filings/\"},
          {\"Name\": \"Suffix\", \"Value\": \"analysis.json\"}
        ]}}
      },
      {
        \"Id\": \"dispatch-filings-extracted\",
        \"LambdaFunctionArn\": \"${dispatch_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/filings/\"},
          {\"Name\": \"Suffix\", \"Value\": \"extracted.json\"}
        ]}}
      },
      {
        \"Id\": \"dispatch-press-releases-extracted\",
        \"LambdaFunctionArn\": \"${dispatch_arn}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [
          {\"Name\": \"Prefix\", \"Value\": \"data/raw/press_releases/\"},
          {\"Name\": \"Suffix\", \"Value\": \"extracted.json\"}
        ]}}
      }
    ]
  }"

# If IAM policy edit was not permitted, this lambda resource policy still enables invoke.
aws lambda add-permission \
  --function-name "${MONITOR_EVALUATOR_FUNCTION}" \
  --statement-id "allow-dispatch-role-invoke" \
  --action lambda:InvokeFunction \
  --principal "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --region "${REGION}" >/dev/null 2>&1 || true

aws lambda add-permission \
  --function-name "${ANALYZER_FUNCTION}" \
  --statement-id "allow-dispatch-role-invoke-analyzer" \
  --action lambda:InvokeFunction \
  --principal "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --region "${REGION}" >/dev/null 2>&1 || true

echo "--- Removing legacy EventBridge rules (best effort) ---"
for rule in "${LEGACY_RULES[@]}"; do
  aws events remove-targets --rule "$rule" --ids "$rule" --region "$REGION" >/dev/null 2>&1 || true
  aws events delete-rule --name "$rule" --region "$REGION" >/dev/null 2>&1 || true
done

echo "--- Removing legacy Lambdas (best effort) ---"
for f in "${LEGACY_FUNCTIONS[@]}"; do
  aws lambda delete-function --function-name "$f" --region "$REGION" >/dev/null 2>&1 || true
done

echo "=== Deploy complete ==="
echo "Functions: ${SEC_POLLER_FUNCTION}, ${PRESS_POLLER_FUNCTION}, ${EXTRACTOR_FUNCTION}, ${ANALYZER_FUNCTION}, ${ALERTS_FUNCTION}, ${DISPATCH_FUNCTION}"
echo "Rules: ${SEC_POLLER_RULE}, ${PRESS_POLLER_RULE}"
echo "Next: run scripts/release_smoke_check.sh"
