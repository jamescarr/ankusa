//! Consume the webhooks Ankusa publishes to NATS JetStream.
//!
//! Each message is Ankusa's queue message JSON (the same bytes its Kafka and
//! RabbitMQ sinks publish). A small body rides inline as `body_base64`; a large
//! one is checked in to the claim-check gateway, and the message carries a
//! `claim` ref plus the body's `sha256` instead. The `ankusa` crate redeems the
//! ref and verifies the bytes.
//!
//! Delivery is at-least-once, so the worker dedupes on `id`.

use std::collections::HashSet;
use std::time::Duration;

use ankusa::ClaimCheckClient;
use ankusa::bytes::Bytes;
use async_nats::jetstream::{self, AckKind, consumer, stream};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::StreamExt;
use serde::Deserialize;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The fields of Ankusa's queue message this worker reads. Others are ignored.
#[derive(Deserialize)]
struct Hook {
    id: String,
    source_id: String,
    body_base64: Option<String>,
    claim: Option<String>,
    sha256: Option<String>,
}

/// Why a message wasn't handled, which decides how JetStream is answered.
enum Failure {
    /// Trying again later can work, e.g. the claim-check gateway was
    /// unreachable: `nak` it, and JetStream redelivers it after a delay.
    Retry(Error),
    /// Every redelivery would fail the same way, e.g. bad JSON or a sha256
    /// mismatch: `term` it, and JetStream stops redelivering it.
    GiveUp(Error),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let nats_url = std::env::var("NATS_URL").unwrap_or("nats://localhost:4222".into());
    let claim_check_url =
        std::env::var("CLAIM_CHECK_URL").unwrap_or("http://localhost:4001".into());
    let claim_check = ClaimCheckClient::new(claim_check_url)?;
    let js = jetstream::new(async_nats::connect(nats_url).await?);

    // The worker owns the topology: Ankusa publishes to a subject and never
    // creates a stream, so one must cover `ankusa.>` before hooks arrive.
    let stream = js
        .get_or_create_stream(stream::Config {
            name: "ANKUSA".into(),
            subjects: vec!["ankusa.>".into()],
            ..Default::default()
        })
        .await?;
    let consumer = stream
        .get_or_create_consumer(
            "worker",
            consumer::pull::Config {
                durable_name: Some("worker".into()),
                ..Default::default()
            },
        )
        .await?;
    println!("worker consuming ankusa.> from stream ANKUSA");

    // In memory for the demo. A real worker records handled ids durably (a
    // unique key in its database), so a redelivery is a no-op across restarts.
    let mut handled = HashSet::new();

    let mut messages = consumer.messages().await?;
    while let Some(message) = messages.next().await {
        // A missed heartbeat or a failed pull is logged, not fatal. If the
        // consumer is deleted, the stream reports it once and then ends.
        let message = match message {
            Ok(message) => message,
            Err(err) => {
                eprintln!("receive failed: {err}");
                continue;
            }
        };
        let ack = match process(&message.payload, &claim_check, &mut handled).await {
            Ok(()) => AckKind::Ack,
            Err(Failure::Retry(err)) => {
                eprintln!("will retry in 5s: {err}");
                AckKind::Nak(Some(Duration::from_secs(5)))
            }
            Err(Failure::GiveUp(err)) => {
                eprintln!("giving up on message: {err}");
                AckKind::Term
            }
        };
        // A lost ack means JetStream redelivers the message later; the dedupe
        // covers a hook that was already handled.
        if let Err(err) = message.ack_with(ack).await {
            eprintln!("ack failed: {err}");
        }
    }
    Err("message stream ended".into())
}

async fn process(
    payload: &[u8],
    claim_check: &ClaimCheckClient,
    handled: &mut HashSet<String>,
) -> Result<(), Failure> {
    let hook: Hook = serde_json::from_slice(payload).map_err(|err| Failure::GiveUp(err.into()))?;
    if handled.contains(&hook.id) {
        println!("duplicate id={} (already handled)", hook.id);
        return Ok(());
    }

    let (body, via) = match (&hook.body_base64, &hook.claim, &hook.sha256) {
        (Some(inline), _, _) => {
            let body = BASE64
                .decode(inline)
                .map_err(|err| Failure::GiveUp(err.into()))?;
            (Bytes::from(body), "inline")
        }
        // `redeem` fetches the body and checks it against `sha256`. Only an
        // unreachable gateway is worth retrying: a missing claim or a digest
        // mismatch fails the same way every time.
        (None, Some(claim), Some(sha256)) => match claim_check.redeem(claim, sha256).await {
            Ok(body) => (body, "claim"),
            Err(err) if err.is_retryable() => return Err(Failure::Retry(err.into())),
            Err(err) => return Err(Failure::GiveUp(err.into())),
        },
        _ => {
            return Err(Failure::GiveUp(
                "no body_base64, and no claim + sha256".into(),
            ));
        }
    };

    // Your business logic goes here. On failure, return `Failure::Retry` if
    // trying again later can work (a database outage), `Failure::GiveUp` if it
    // can't. The id is recorded only after success, so a retried hook is
    // handled again instead of being skipped as a duplicate.
    let preview = String::from_utf8_lossy(&body[..body.len().min(80)]);
    println!(
        "received id={} source={} via={via} bytes={} body={preview}",
        hook.id,
        hook.source_id,
        body.len(),
    );
    handled.insert(hook.id);
    Ok(())
}
