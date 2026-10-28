"""Consume the webhooks Ankusa produces to Kafka, from the SQS FIFO queue a
Redpanda Connect bridge fills from the topic. Uses the `ankusa` SDK
(packages/sdk-python, a uv path dependency, see pyproject.toml) to decode each
message, read its idempotency key, and redeem a large body from the claim-check
gateway.

Every message is Ankusa's queue message JSON (the same bytes its RabbitMQ and
NATS sinks publish). A body under `inline_max_bytes` rides inline as
`body_base64`; a larger one is checked in, and the message carries a `claim` ref
plus the body's `sha256` instead. Delivery is at-least-once, so the worker
dedupes on `idempotency_key(message)`. A message leaves the queue only once it
is handled: a failure a retry can fix (the gateway unreachable) backs it off,
one every redelivery would repeat (bad JSON, a sha256 mismatch) goes to the
dead-letter queue so the rest of its FIFO group keeps moving.
"""

import base64
import os

import boto3
from ankusa import ClaimCheckClient, ClaimCheckError, InvalidMessageError, decode_message, idempotency_key

QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DLQ_URL = os.environ["SQS_DLQ_URL"]
CLAIM_CHECK_URL = os.environ.get("CLAIM_CHECK_URL", "http://localhost:4001")

# Region and credentials come from AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY; SQS_ENDPOINT is floci in docker-compose.yml, unset for AWS.
sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT"))
# The gateway is open in this demo: no bearer token. Auth belongs in front of it.
claim_check = ClaimCheckClient(CLAIM_CHECK_URL)

# In memory for the demo. A real worker records handled keys durably (a unique
# key in its database), so a redelivery is a no-op even across restarts.
handled: set[str] = set()


class PermanentError(Exception):
    """Retrying would fail the same way: dead-letter now so the FIFO group moves."""


def handle(source: str, body: bytes) -> None:
    """Your business logic goes here."""


def process(raw: str) -> None:
    try:
        message = decode_message(raw)
    except InvalidMessageError as err:
        raise PermanentError(f"invalid message ({err.code}): {err}") from err

    key = idempotency_key(message)
    if key in handled:
        print(f"duplicate key={key} source={message.source_id} (already handled)", flush=True)
        return

    if message.claim:
        # `redeem` fetches the body and checks it against `sha256`. Only an
        # unreachable gateway is worth retrying: a missing claim or a digest
        # mismatch fails the same way every time.
        try:
            body = claim_check.redeem(message.claim, message.sha256)
        except ClaimCheckError as err:
            if err.retryable:
                raise
            raise PermanentError(f"claim not redeemable: {err}") from err
        via = "claim"
    else:
        body = base64.b64decode(message.body_base64)
        via = "inline"

    handle(message.source_id, body)
    handled.add(key)
    print(
        f"received id={message.id} source={message.source_id} via={via} "
        f"bytes={len(body)} body={body[:200].decode('utf-8', 'replace')}",
        flush=True,
    )


def delete(m: dict) -> None:
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])


def dead_letter(m: dict, reason: str) -> None:
    # Send, then delete: a crash in between duplicates the message in the DLQ
    # rather than losing it.
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=m["Body"],
        MessageGroupId=m["Attributes"]["MessageGroupId"],
        MessageDeduplicationId=m["MessageId"],
        MessageAttributes={"failure_reason": {"DataType": "String", "StringValue": reason}},
    )
    delete(m)


def back_off(m: dict) -> None:
    receives = int(m["Attributes"].get("ApproximateReceiveCount", "1"))
    sqs.change_message_visibility(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=m["ReceiptHandle"],
        VisibilityTimeout=min(30 * 2 ** (receives - 1), 900),
    )


def main() -> None:
    print(f"worker polling {QUEUE_URL}, redeeming claims via {CLAIM_CHECK_URL}", flush=True)
    while True:
        messages = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageSystemAttributeNames=["ApproximateReceiveCount", "MessageGroupId"],
        ).get("Messages", [])
        # A group with a transient failure in this batch: its later messages
        # back off too, or they would be processed out of order.
        blocked: set[str] = set()
        for m in messages:
            group = m["Attributes"]["MessageGroupId"]
            if group in blocked:
                back_off(m)
                continue
            try:
                process(m["Body"])
                delete(m)
            except PermanentError as err:
                print(f"dead-lettering: {err}", flush=True)
                dead_letter(m, str(err))
            except Exception as err:  # noqa: BLE001 - anything else is worth a retry
                print(f"will retry: {err}", flush=True)
                blocked.add(group)
                back_off(m)


if __name__ == "__main__":
    main()
