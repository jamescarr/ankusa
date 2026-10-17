#!/bin/sh
# Runs next to floci (same pod, so localhost:4566), forever:
#
#  1. Makes sure the claim bucket exists. floci keeps everything in memory, so a
#     restarted floci is an empty one; without this the ingest pods would answer
#     503 until someone re-ran the install.
#  2. Deletes claim objects older than $RETENTION_MINUTES. floci never expires
#     anything on its own at this scale (its lifecycle rules count in days), and
#     the bodies live in its memory: without this, a few minutes of large hooks
#     fill it. A hook not consumed within the retention window can no longer
#     redeem its claim (the consumer terminates it).
set -eu

: "${BUCKET:?}" "${RETENTION_MINUTES:?}"
export AWS_ENDPOINT_URL=http://localhost:4566

until aws s3 ls >/dev/null 2>&1; do echo "waiting for floci..."; sleep 1; done

while true; do
  if ! aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    aws s3 mb "s3://$BUCKET" >/dev/null && echo "created bucket $BUCKET"
  fi

  # ISO-8601 timestamps compare correctly as strings.
  cutoff=$(date -u -d "-$RETENTION_MINUTES minutes" +%Y-%m-%dT%H:%M:%S)
  aws s3api list-objects-v2 --bucket "$BUCKET" \
    --query 'Contents[].[LastModified,Key]' --output text 2>/dev/null |
    awk -v cutoff="$cutoff" '$1 != "None" && $1 < cutoff { print $2 }' |
    xargs -r -n 500 sh -c '
      objects=$(printf "{\"Key\":\"%s\"}," "$@")
      aws s3api delete-objects --bucket "$BUCKET" \
        --delete "{\"Quiet\":true,\"Objects\":[${objects%,}]}" >/dev/null &&
        echo "expired $# claim objects"
    ' sh || true

  sleep 30
done
