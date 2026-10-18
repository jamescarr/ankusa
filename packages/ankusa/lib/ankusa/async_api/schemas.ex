defmodule Ankusa.AsyncApi.SinkMessage do
  @moduledoc """
  The JSON body every queue-style sink publishes: `Ankusa.Sink.Message` version 1,
  as an AsyncAPI schema. Declared once; `Ankusa.AsyncApi` refers to it from every
  message.
  """

  use AsyncApiSpex.Schema,
    name: "SinkMessageV1",
    schema: %{
      "type" => "object",
      "description" =>
        "One delivered hook. A body of at most `inline_max_bytes` (default 64 KiB) rides " <>
          "inline as `body_base64`; a larger one is stored in the claim check and the message " <>
          "carries its reference as `claim` plus the lowercase hex `sha256` to check the " <>
          "redeemed bytes against. `v` changes only when an existing field changes meaning or " <>
          "disappears: adding a field keeps `v: 1`, so consumers must ignore keys they do not know.",
      "required" => [
        "v",
        "id",
        "source_id",
        "received_at",
        "size",
        "sha256",
        "dedupe_key",
        "replay_id",
        "idempotency_key",
        "verification",
        "headers"
      ],
      "properties" => %{
        "v" => %{"const" => 1, "description" => "Wire format version."},
        "id" => %{
          "type" => "string",
          "description" =>
            "The hook's id (a UUIDv7). Delivery is at-least-once: dedupe on `idempotency_key`."
        },
        "source_id" => %{"type" => "string"},
        "tenant_id" => %{"type" => ["string", "null"]},
        "received_at" => %{
          "type" => "integer",
          "description" => "When Ankusa accepted the hook, Unix milliseconds."
        },
        "content_type" => %{
          "type" => ["string", "null"],
          "description" => "The content type the provider sent the hook with, not this message's."
        },
        "size" => %{
          "type" => "integer",
          "minimum" => 0,
          "description" => "Bytes in the hook body."
        },
        "dedupe_key" => %{
          "type" => ["string", "null"],
          "description" =>
            "The provider event key extracted at ingest, or `null`. Null does not mean no key: " <>
              "check whether the source is configured with one."
        },
        "replay_id" => %{
          "type" => ["string", "null"],
          "description" => "The replay job id when this delivery is a replay, else `null`."
        },
        "idempotency_key" => %{
          "type" => "string",
          "description" =>
            "The tenant-scoped key a consumer dedupes on: `tenant:source_id:dedupe_key` when a " <>
              "dedupe key was extracted, else `id`. Read this; do not rebuild it."
        },
        "verification" => %{
          "type" => "object",
          "required" => ["status", "flagged"],
          "properties" => %{
            "status" => %{"enum" => ["ok", "failed", "skipped"]},
            "flagged" => %{"type" => "boolean"}
          },
          "description" =>
            "How the hook was verified at ingest. `flagged: true` means the signature check " <>
              "failed and the source's `on_verify_failure: accept_flag` let it through: treat " <>
              "the hook, and the tenant it names, as unverified. `skipped` is a hook nothing " <>
              "checked: a source with no verifier, or a lifecycle event."
        },
        "headers" => %{
          "type" => "object",
          "description" =>
            "Forwarded provider request headers, lowercased; repeated names joined with `, `. " <>
              "`{}` when the source forwards none."
        },
        "body_base64" => %{
          "type" => "string",
          "contentEncoding" => "base64",
          "description" =>
            "The hook body, verbatim. Present when the body is small enough to inline."
        },
        "claim" => %{
          "type" => "string",
          "pattern" => "^urn:ankusa:claim:v1:[A-Za-z0-9_-]{1,64}:.+$",
          "description" => "Claim-check reference to the body, redeemable at the claim-check API."
        },
        "sha256" => %{
          "type" => "string",
          "pattern" => "^[0-9a-f]{64}$",
          "description" => "Hex SHA-256 of the body, on every message."
        }
      },
      "oneOf" => [
        %{"required" => ["body_base64"]},
        %{"required" => ["claim"]}
      ]
    }
end

defmodule Ankusa.AsyncApi.SinkMessageHeaders do
  @moduledoc """
  The application headers Kafka and NATS carry beside a `SinkMessageV1` body.
  Redis carries none; RabbitMQ sends `ankusa_idempotency_key` (and the optional
  dedupe/replay keys) as AMQP headers, which this schema does not describe for it.
  """

  use AsyncApiSpex.Schema,
    name: "SinkMessageHeadersV1",
    schema: %{
      "type" => "object",
      "required" => [
        "ankusa_id",
        "ankusa_source_id",
        "ankusa_tenant_id",
        "ankusa_message_version",
        "ankusa_idempotency_key",
        "content_type"
      ],
      "properties" => %{
        "ankusa_id" => %{"type" => "string", "description" => "Same as the body's `id`."},
        "ankusa_source_id" => %{"type" => "string"},
        "ankusa_tenant_id" => %{
          "type" => "string",
          "description" => "The tenant, or the empty string when there is none."
        },
        "ankusa_message_version" => %{"const" => "1"},
        "content_type" => %{"const" => "application/json"},
        "ankusa_idempotency_key" => %{
          "type" => "string",
          "description" => "Same value as the message's `idempotency_key`."
        },
        "ankusa_dedupe_key" => %{
          "type" => "string",
          "description" => "The provider event key, when the hook carries one."
        },
        "ankusa_replay_id" => %{
          "type" => "string",
          "description" => "The replay job id, when this delivery is a replay."
        },
        "Nats-Msg-Id" => %{
          "type" => "string",
          "description" =>
            "NATS only: `id`, or `id:replay:<replay_id>` on a replay, so JetStream's " <>
              "duplicate window collapses lost-ack retries of one delivery."
        }
      }
    }
end

defmodule Ankusa.AsyncApi.LifecycleEvent do
  @moduledoc """
  A lifecycle event (`Ankusa.Lifecycle`): a CloudEvents 1.0 structured-mode
  event. It is the decoded `body_base64` of a lifecycle channel's `SinkMessageV1`.
  """

  use AsyncApiSpex.Schema,
    name: "LifecycleEventV1",
    schema: %{
      "type" => "object",
      "description" =>
        "A webhook endpoint (source) or route was created, updated, or deleted. " <>
          "`data` is the entity as the admin API shows it, secrets redacted, as the change " <>
          "left it; for a deletion, as it was the moment before. Delivery is at least once and " <>
          "in commit order: `id` is derived from the change, so the same event always has " <>
          "the same id, and `revision` lets a consumer ignore a stale update.",
      "required" => [
        "specversion",
        "id",
        "source",
        "type",
        "subject",
        "time",
        "datacontenttype",
        "data"
      ],
      "properties" => %{
        "specversion" => %{"const" => "1.0"},
        "id" => %{"type" => "string"},
        "source" => %{"type" => "string", "pattern" => "^urn:ankusa:instance:"},
        "type" => %{
          "enum" => [
            "io.ankusa.source.created",
            "io.ankusa.source.updated",
            "io.ankusa.source.deleted",
            "io.ankusa.route.created",
            "io.ankusa.route.updated",
            "io.ankusa.route.deleted"
          ]
        },
        "subject" => %{
          "type" => "string",
          "description" => "The source id (`<tenant>.<name>`) or the route id."
        },
        "time" => %{"type" => "string", "format" => "date-time"},
        "datacontenttype" => %{"const" => "application/json"},
        "data" => %{"type" => "object"},
        "revision" => %{
          "type" => "integer",
          "minimum" => 1,
          "description" =>
            "The subject's change counter, 1 at creation. Absent for a store that keeps none."
        },
        "actor" => %{
          "type" => "string",
          "description" => "The id of the principal that made the change (`system` for code)."
        },
        "instance" => %{"type" => "string", "description" => "The instance's name."},
        "sequence" => %{
          "type" => "string",
          "pattern" => "^[0-9]+$",
          "description" => "The instance's audit sequence number: events are ordered by it."
        },
        "replayid" => %{
          "type" => "string",
          "description" => "Present only on an event a replay job re-sent: the job's id."
        }
      }
    }
end
