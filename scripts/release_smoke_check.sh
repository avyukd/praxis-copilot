#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION:-us-east-2}"
S3_BUCKET="${S3_BUCKET:-praxis-copilot}"

FUNCS=(
  "8k-scanner-poller"
  "8k-scanner-extractor"
  "8k-scanner-analyzer"
  "8k-scanner-ca-poller"
  "8k-scanner-ca-analyzer"
  "8k-scanner-us-gnw-poller"
  "8k-scanner-us-gnw-analyzer"
  "8k-scanner-dispatch"
)

echo "## Lambda state"
for f in "${FUNCS[@]}"; do
  state=$(aws lambda get-function-configuration --region "$REGION" --function-name "$f" --query 'State' --output text)
  echo "$f: $state"
done

echo

echo "## DryRun invoke"
for f in "${FUNCS[@]}"; do
  if aws lambda invoke --region "$REGION" --function-name "$f" --invocation-type DryRun /tmp/dryrun.out >/dev/null 2>&1; then
    echo "$f: OK"
  else
    echo "$f: FAIL"
  fi
done

echo

echo "## S3 notification routes"
aws s3api get-bucket-notification-configuration --region "$REGION" --bucket "$S3_BUCKET"

echo

echo "## EventBridge rules"
for r in 8k-scanner-poller-cron 8k-scanner-ca-poller-cron 8k-scanner-us-gnw-poller-cron; do
  aws events describe-rule --region "$REGION" --name "$r" --query '{Name:Name,State:State,ScheduleExpression:ScheduleExpression}'
done
