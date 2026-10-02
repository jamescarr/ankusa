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
      "required" => ["v", "id", "source_id", "received_at", "size"],
      "properties" => %{
        "v" => %{"const" => 1, "description" => "Wire format version."},
        "id" => %{
          "type" => "string",
          "description" => "The hook's id (a UUIDv7). Delivery is at-least-once: dedupe on it."
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
          "description" => "Hex SHA-256 of the body. Present with `claim`."
        }
      },
      "oneOf" => [
        %{"required" => ["body_base64"]},
        %{"required" => ["claim", "sha256"]}
      ]
    }
end

defmodule Ankusa.AsyncApi.SinkMessageHeaders do
  @moduledoc """
  The application headers Kafka and NATS carry beside a `SinkMessageV1` body.
  RabbitMQ and Redis carry none.
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
        "content_type" => %{"const" => "application/json"}
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
          "`data` is the entity as the admin API shows it, secrets redacted; for a deletion " <>
          "it is the last view of a source, or `{\"id\": ...}` for a route.",
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
        "data" => %{"type" => "object"}
      }
    }
end
