#!/bin/bash
# Deploy the monitor system: evaluator Lambda, EventBridge rules, S3 notifications
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="praxis-copilot"
LAMBDA_PREFIX="praxis"

echo "=== Deploying Monitor System ==="
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"

# --- 1. Deploy monitor-evaluator Lambda ---
echo ""
echo "--- Deploying monitor-evaluator Lambda ---"

EVALUATOR_FUNCTION="${LAMBDA_PREFIX}-monitor-evaluator"

# Check if function exists
if aws lambda get-function --function-name "$EVALUATOR_FUNCTION" --region "$REGION" 2>/dev/null; then
    echo "Updating existing Lambda: $EVALUATOR_FUNCTION"
    aws lambda update-function-code \
        --function-name "$EVALUATOR_FUNCTION" \
        --zip-file fileb://dist/monitor-evaluator.zip \
        --region "$REGION" \
        --no-cli-pager
else
    echo "Creating new Lambda: $EVALUATOR_FUNCTION"
    aws lambda create-function \
        --function-name "$EVALUATOR_FUNCTION" \
        --runtime python3.13 \
        --handler src.modules.monitor.evaluator.handler.handler \
        --role "arn:aws:iam::${ACCOUNT_ID}:role/praxis-lambda-role" \
        --zip-file fileb://dist/monitor-evaluator.zip \
        --timeout 300 \
        --memory-size 512 \
        --environment "Variables={S3_BUCKET=${BUCKET},SNS_TOPIC_ARN=${SNS_TOPIC_ARN:-}}" \
        --region "$REGION" \
        --no-cli-pager
fi

# --- 2. EventBridge: daily scheduled monitor run (search + scraper types) ---
echo ""
echo "--- Setting up EventBridge daily schedule ---"

RULE_NAME="${LAMBDA_PREFIX}-monitor-daily"

aws events put-rule \
    --name "$RULE_NAME" \
    --schedule-expression "cron(0 14 ? * MON-FRI *)" \
    --state ENABLED \
    --description "Daily monitor evaluation (search + scraper monitors)" \
    --region "$REGION" \
    --no-cli-pager

aws events put-targets \
    --rule "$RULE_NAME" \
    --targets "Id=monitor-evaluator,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${EVALUATOR_FUNCTION},Input={\"trigger_type\":\"scheduled\"}" \
    --region "$REGION" \
    --no-cli-pager

# Grant EventBridge permission to invoke the Lambda
aws lambda add-permission \
    --function-name "$EVALUATOR_FUNCTION" \
    --statement-id "eventbridge-daily-${RULE_NAME}" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region "$REGION" \
    --no-cli-pager 2>/dev/null || true

# --- 3. S3 notification: unified filings path -> dispatch ---
echo ""
echo "--- Configuring S3 notifications for unified filing path ---"

DISPATCH_FUNCTION="${LAMBDA_PREFIX}-dispatch"

# Note: S3 notification configuration must be done as a complete replacement.
# This adds the filings/ prefix trigger alongside existing triggers.
# See deploy_8k_events.sh for the full notification config.

echo "IMPORTANT: Update the S3 notification configuration in deploy_8k_events.sh"
echo "to include the new prefix: data/raw/filings/ -> ${DISPATCH_FUNCTION}"

# --- 4. Sync monitor configs to S3 ---
echo ""
echo "--- Syncing monitor configs to S3 ---"

if [ -d "config/monitors" ] && [ "$(ls -A config/monitors/*.yaml 2>/dev/null)" ]; then
    aws s3 sync config/monitors/ "s3://${BUCKET}/config/monitors/" \
        --region "$REGION" \
        --no-cli-pager
    echo "Synced monitor configs to S3"
else
    echo "No monitor configs to sync"
fi

# Sync scraper scripts if any
if [ -d "config/scrapers" ] && [ "$(ls -A config/scrapers/*.py 2>/dev/null)" ]; then
    aws s3 sync config/scrapers/ "s3://${BUCKET}/config/scrapers/" \
        --region "$REGION" \
        --no-cli-pager
    echo "Synced scraper scripts to S3"
fi

echo ""
echo "=== Monitor System Deployment Complete ==="
echo "  Evaluator Lambda: $EVALUATOR_FUNCTION"
echo "  Daily schedule: $RULE_NAME (weekdays 14:00 UTC)"
echo ""
echo "Next steps:"
echo "  1. Update deploy_8k_events.sh S3 notifications for data/raw/filings/"
echo "  2. Create monitors: praxis monitor add"
echo "  3. Sync config: praxis config sync"
