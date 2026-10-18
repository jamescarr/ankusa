-- The table `mix loadgen.verify` reads: the same shape the oban-consumer's
-- migration creates, plus `via` (how the body reached the consumer).
CREATE TABLE processed_webhooks (
  -- The idempotency key the hook message carries (`tenant:source:dedupe_key`,
  -- the hook id when there is no dedupe key). The primary key, so a provider
  -- retry that landed on the other node collapses into one row.
  idempotency_key text PRIMARY KEY,
  ankusa_id       text NOT NULL,
  source_id       text NOT NULL,
  tenant_id       text,
  body            bytea NOT NULL,
  body_sha256     text NOT NULL,
  deliveries      integer NOT NULL DEFAULT 1,
  -- Set once the body is in hand and recorded; loadgen.verify counts a row
  -- only when it is set.
  processed_at    timestamptz,
  -- `inline`, or `claim:<claim_id>` when the body was redeemed at the gateway.
  via             text NOT NULL DEFAULT 'inline'
);

CREATE INDEX processed_webhooks_ankusa_id ON processed_webhooks (ankusa_id);
