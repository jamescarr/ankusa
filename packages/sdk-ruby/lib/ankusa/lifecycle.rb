# frozen_string_literal: true

require "json"

module Ankusa
  # One decoded lifecycle event: the CloudEvents 1.0 (structured mode) event an
  # Ankusa instance publishes when a source or a route is created, updated or
  # deleted, when a tenant is erased, or when the instance is restored from a
  # backup. It arrives as the body of a queue message from the instance's
  # lifecycle sinks (content type `application/cloudevents+json`, `source_id`
  # `ankusa:lifecycle`): decode the message with `Ankusa.decode_message`,
  # redeem its body, and hand the bytes to `Ankusa.decode_lifecycle`.
  #
  # Delivery is at least once and in commit order. `id` is derived from the
  # change, so the same event always has the same id, and `revision` (per
  # `subject`) lets a consumer ignore a stale update. `sequence` is the
  # instance's audit sequence number, the decimal string it is on the wire.
  # `data` is the entity as the admin API shows it, secrets redacted (nil when
  # the event carries none). An optional field the event omitted or said `null`
  # is nil.
  LifecycleEvent = Data.define(
    :specversion, :id, :source, :type, :subject, :time, :datacontenttype, :data, :revision,
    :actor, :instance, :sequence, :replayid, :traceparent, :tracestate
  )

  LIFECYCLE_REQUIRED_FIELDS = %w[id source type subject time].freeze
  LIFECYCLE_DECIMAL = /\A[0-9]+\z/
  private_constant :LIFECYCLE_REQUIRED_FIELDS, :LIFECYCLE_DECIMAL

  # Decodes one lifecycle event (the JSON of the CloudEvent) into a
  # `LifecycleEvent`. The checks run in a fixed order and the first failure
  # wins, raising `InvalidMessageError` (never retryable): `invalid_json`,
  # `not_an_object`, `unsupported_version` (`specversion` is not "1.0") or
  # `invalid_field` with the offending `field`. Keys and event types this SDK
  # does not know are ignored, so a consumer keeps working when a newer node
  # announces more.
  def self.decode_lifecycle(data)
    parsed =
      begin
        JSON.parse(data)
      rescue JSON::ParserError
        raise InvalidMessageError.new("event is not JSON", code: "invalid_json")
      end

    unless parsed.is_a?(Hash)
      raise InvalidMessageError.new("event is not a JSON object", code: "not_an_object")
    end

    unless parsed["specversion"] == "1.0"
      raise InvalidMessageError.new("unsupported specversion", code: "unsupported_version")
    end

    LIFECYCLE_REQUIRED_FIELDS.each do |field|
      value = parsed[field]
      lifecycle_invalid_field(field) unless value.is_a?(String) && !value.empty?
    end

    datacontenttype = lifecycle_optional_string(parsed, "datacontenttype")

    body = parsed["data"]
    lifecycle_invalid_field("data") unless body.nil? || body.is_a?(Hash)

    revision = parsed["revision"]
    lifecycle_invalid_field("revision") unless revision.nil? || lifecycle_revision?(revision)

    actor = lifecycle_optional_string(parsed, "actor")
    instance = lifecycle_optional_string(parsed, "instance")

    sequence = parsed["sequence"]
    lifecycle_invalid_field("sequence") unless sequence.nil? || lifecycle_decimal?(sequence)

    LifecycleEvent.new(
      specversion: "1.0",
      id: parsed["id"],
      source: parsed["source"],
      type: parsed["type"],
      subject: parsed["subject"],
      time: parsed["time"],
      datacontenttype: datacontenttype,
      data: body,
      revision: revision,
      actor: actor,
      instance: instance,
      sequence: sequence,
      replayid: lifecycle_optional_string(parsed, "replayid"),
      traceparent: lifecycle_optional_string(parsed, "traceparent"),
      tracestate: lifecycle_optional_string(parsed, "tracestate")
    )
  end

  def self.lifecycle_optional_string(parsed, field)
    value = parsed[field]
    lifecycle_invalid_field(field) unless value.nil? || value.is_a?(String)
    value
  end
  private_class_method :lifecycle_optional_string

  def self.lifecycle_revision?(value)
    value.is_a?(Integer) && value >= 1
  end
  private_class_method :lifecycle_revision?

  def self.lifecycle_decimal?(value)
    value.is_a?(String) && LIFECYCLE_DECIMAL.match?(value)
  end
  private_class_method :lifecycle_decimal?

  def self.lifecycle_invalid_field(field)
    raise InvalidMessageError.new("invalid or missing field: #{field}", code: "invalid_field", field: field)
  end
  private_class_method :lifecycle_invalid_field
end
