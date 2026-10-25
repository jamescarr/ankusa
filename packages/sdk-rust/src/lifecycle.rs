//! Decoding a lifecycle event: the `CloudEvents` 1.0 (structured mode) event an
//! Ankusa instance publishes when a source or a route is created, updated or
//! deleted, when a tenant is erased, or when the instance is restored from a
//! backup.
//!
//! It arrives as the body of a queue message from the instance's lifecycle
//! sinks (content type `application/cloudevents+json`, `source_id`
//! `ankusa:lifecycle`): decode the message with
//! [`decode_message`](crate::decode_message), redeem its body, and hand the
//! bytes to [`decode_lifecycle`].

use serde_json::{Map, Value};

use crate::message::InvalidMessageError;

/// A decoded lifecycle event.
///
/// Delivery is at least once and in commit order. `id` is derived from the
/// change, so the same event always has the same id, and `revision` (per
/// `subject`) lets a consumer ignore a stale update. An optional field is
/// `None` when the event omitted it or said `null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent {
    /// The `CloudEvents` version. Always `"1.0"`.
    pub specversion: String,
    /// The event id, derived from the change.
    pub id: String,
    /// `urn:ankusa:instance:<instance>`.
    pub source: String,
    /// `io.ankusa.source.created`, `io.ankusa.route.updated`, …; a type this
    /// SDK does not know passes through.
    pub r#type: String,
    /// The source id, the route id, the erased tenant or the instance name.
    pub subject: String,
    /// When the change committed, as the wire says it (an RFC 3339 string, not
    /// parsed).
    pub time: String,
    /// The data's content type.
    pub datacontenttype: Option<String>,
    /// The entity as the admin API shows it, secrets redacted; `None` when the
    /// event carries none.
    pub data: Option<Map<String, Value>>,
    /// The subject's change counter, at least 1.
    pub revision: Option<u64>,
    /// The principal that made the change (`system` for code).
    pub actor: Option<String>,
    /// The instance's name.
    pub instance: Option<String>,
    /// The instance's audit sequence number, the decimal string it is on the
    /// wire.
    pub sequence: Option<String>,
    /// The replay job id, on an event a replay re-emitted.
    pub replayid: Option<String>,
    /// The W3C `traceparent` of the span that emitted the event.
    pub traceparent: Option<String>,
    /// The W3C `tracestate` that goes with `traceparent`.
    pub tracestate: Option<String>,
}

/// Decodes one lifecycle event (the JSON of the `CloudEvent`).
///
/// Unknown keys and unknown event types are ignored.
///
/// # Errors
///
/// [`InvalidMessageError`], never retryable, with `code` (and `field` for
/// `invalid_field`): `invalid_json`, `not_an_object`, `unsupported_version`
/// (`specversion` is not `"1.0"`) or `invalid_field`. The checks run in that
/// order, and within `invalid_field` in the order `id`, `source`, `type`,
/// `subject`, `time`, `datacontenttype`, `data`, `revision`, `actor`,
/// `instance`, `sequence`, `replayid`, `traceparent`, `tracestate`; the first
/// failure wins.
pub fn decode_lifecycle(data: &[u8]) -> Result<LifecycleEvent, InvalidMessageError> {
    let value: Value = serde_json::from_slice(data).map_err(|_| invalid("invalid_json", None))?;
    let Value::Object(map) = value else {
        return Err(invalid("not_an_object", None));
    };

    if map.get("specversion").and_then(Value::as_str) != Some("1.0") {
        return Err(invalid("unsupported_version", None));
    }

    let id = required_string(&map, "id")?;
    let source = required_string(&map, "source")?;
    let r#type = required_string(&map, "type")?;
    let subject = required_string(&map, "subject")?;
    let time = required_string(&map, "time")?;
    let datacontenttype = optional_string(&map, "datacontenttype")?;

    let body = match map.get("data") {
        None | Some(Value::Null) => None,
        Some(Value::Object(body)) => Some(body.clone()),
        Some(_) => return Err(invalid("invalid_field", Some("data"))),
    };

    let revision = match map.get("revision") {
        None | Some(Value::Null) => None,
        Some(value) => match value.as_u64() {
            Some(revision) if revision >= 1 => Some(revision),
            _ => return Err(invalid("invalid_field", Some("revision"))),
        },
    };

    let actor = optional_string(&map, "actor")?;
    let instance = optional_string(&map, "instance")?;

    let sequence = optional_string(&map, "sequence")?;
    if sequence.as_deref().is_some_and(|sequence| {
        sequence.is_empty() || !sequence.bytes().all(|b| b.is_ascii_digit())
    }) {
        return Err(invalid("invalid_field", Some("sequence")));
    }

    Ok(LifecycleEvent {
        specversion: "1.0".to_owned(),
        id,
        source,
        r#type,
        subject,
        time,
        datacontenttype,
        data: body,
        revision,
        actor,
        instance,
        sequence,
        replayid: optional_string(&map, "replayid")?,
        traceparent: optional_string(&map, "traceparent")?,
        tracestate: optional_string(&map, "tracestate")?,
    })
}

fn invalid(code: &'static str, field: Option<&'static str>) -> InvalidMessageError {
    InvalidMessageError::new(code, field)
}

fn required_string(
    map: &Map<String, Value>,
    key: &'static str,
) -> Result<String, InvalidMessageError> {
    match map.get(key) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(invalid("invalid_field", Some(key))),
    }
}

fn optional_string(
    map: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, InvalidMessageError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid("invalid_field", Some(key))),
    }
}
