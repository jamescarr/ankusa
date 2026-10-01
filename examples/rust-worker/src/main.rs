//! Receive webhooks from Ankusa's HTTP sink. Uses the `ankusa` crate
//! (crates.io) to parse the identity Ankusa attaches to every delivery.
//!
//! Ankusa POSTs each hook's raw body here, with its identity in headers:
//! x-ankusa-id (dedupe on this), x-ankusa-source, and x-ankusa-tenant when the
//! source has one. Answer 2xx once the hook is safely handled; anything else (or
//! no answer within 5s) is retried, then dead-lettered for replay.

use std::collections::HashSet;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use tokio::sync::Mutex;

// Ids already handled. In memory for the demo; a real worker records them
// durably (a unique key in its database) so a redelivery is a no-op even
// across restarts.
type Handled = Arc<Mutex<HashSet<String>>>;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let app = Router::new()
        .route("/hooks", post(hooks))
        .with_state(Handled::default());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    println!("worker listening on :{port}/hooks");
    axum::serve(listener, app).await
}

async fn hooks(State(handled): State<Handled>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let Ok(hook) = ankusa::parse_headers(&headers) else {
        return StatusCode::BAD_REQUEST; // no x-ankusa-id
    };

    // `insert` returns false when the id was already there.
    if !handled.lock().await.insert(hook.id.to_owned()) {
        println!(
            "duplicate id={} source={} (already handled)",
            hook.id, hook.source
        );
        return StatusCode::NO_CONTENT;
    }

    // Your business logic goes here.
    let preview = String::from_utf8_lossy(&body[..body.len().min(200)]);
    println!(
        "received id={} source={} bytes={} body={preview}",
        hook.id,
        hook.source,
        body.len(),
    );
    StatusCode::NO_CONTENT
}
