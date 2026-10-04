//! Runs the language-neutral vectors in `conformance/cases/*.json` against
//! this crate's public API — the same 124 cases the TypeScript and Python SDKs
//! pass. `mise run check:conformance` runs it for every SDK; by hand:
//! `cargo test --locked --test conformance`.
//!
//! Only root imports (`ankusa::…`) are used: an export that does not exist
//! yet fails its own cases instead of the whole file, which also makes this
//! runner a public-surface check.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "conformance harness: a malformed vector must abort loudly"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ankusa::bytes::Bytes;
use ankusa::http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use ankusa::{
    AdminClient, AdminError, ClaimCheckClient, ClaimCheckError, ClientBuilder, DryRunRequest,
    InvalidClaimRefError, InvalidMessageError, IpRules, ListDeadLettersParams,
    ListQuarantinedParams, ListRoutesParams, Message, MissingHookIdError, ReplayPatch, ReplaySpec,
    RouteInput, RoutePatch, RoutesClient, RoutesError, Transport, TransportError, decode_message,
    parse_claim_ref, parse_headers,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use libtest_mimic::{Failed, Trial};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// One vector file.
#[derive(Debug, Deserialize)]
struct CaseFile {
    cases: Vec<Case>,
}

/// One vector: what to do, and what must come out.
#[derive(Debug, Deserialize)]
struct Case {
    id: String,
    operation: String,
    input: Value,
    /// Left as JSON: `routes.delete.ok` has `"ok": null`, and every key's
    /// presence is itself an assertion.
    expect: Value,
}

/// One request as it reached the gateway.
#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Value,
}

fn main() -> ExitCode {
    let args = libtest_mimic::Arguments::from_args();
    let trials: Vec<Trial> = cases()
        .into_iter()
        .map(|case| {
            let name = case.id.clone();
            Trial::test(name, move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| Failed::from(format!("tokio runtime: {err}")))?;
                runtime.block_on(run_case(&case))
            })
        })
        .collect();
    libtest_mimic::run(&args, trials).exit_code()
}

/// Every case, in file-name order. Zero cases would otherwise report a green
/// run, so an empty directory fails here.
fn cases() -> Vec<Case> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/cases");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
        .map(|entry| entry.expect("readable directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    files.sort();

    let mut cases = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|err| panic!("reading {}: {err}", file.display()));
        let parsed: CaseFile = serde_json::from_str(&text)
            .unwrap_or_else(|err| panic!("parsing {}: {err}", file.display()));
        cases.extend(parsed.cases);
    }
    assert!(
        !cases.is_empty(),
        "no conformance cases found under {}",
        dir.display()
    );
    cases
}

async fn run_case(case: &Case) -> Result<(), Failed> {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let outcome = dispatch(case, &recorded)
        .await
        .map_err(|err| error_json(&err));
    let expect = case
        .expect
        .as_object()
        .unwrap_or_else(|| panic!("case {}: expect must be an object", case.id));

    if expect.contains_key("ok") {
        let value = outcome.map_err(|error| {
            Failed::from(format!("case {}: expected ok, got error {error}", case.id))
        })?;
        let want = expected_ok(case, &expect["ok"]);
        if value != want {
            return Err(Failed::from(format!(
                "case {}: expected {want}, got {value}",
                case.id
            )));
        }
    } else if expect.contains_key("error") {
        let error = match outcome {
            Ok(value) => {
                return Err(Failed::from(format!(
                    "case {}: expected error {}, got ok {value}",
                    case.id, expect["error"]
                )));
            }
            Err(error) => error,
        };
        let want = expect["error"]
            .as_object()
            .unwrap_or_else(|| panic!("case {}: expect.error must be an object", case.id));
        if error.get("class") != want.get("class") {
            return Err(Failed::from(format!(
                "case {}: expected class {}, got {}",
                case.id,
                want.get("class").unwrap_or(&Value::Null),
                error.get("class").unwrap_or(&Value::Null)
            )));
        }
        for (key, value) in want {
            if key == "class" {
                continue;
            }
            let got = error.get(key);
            if got != Some(value) {
                return Err(Failed::from(format!(
                    "case {}: {} {key}: expected {value}, got {}",
                    case.id,
                    error.get("class").unwrap_or(&Value::Null),
                    got.map_or_else(|| "(absent)".to_owned(), ToString::to_string)
                )));
            }
        }
    } else {
        // Only `requests` is asserted: the operation has to have completed
        // without a mapped error.
        outcome
            .map_err(|error| Failed::from(format!("case {}: unexpected error {error}", case.id)))?;
    }

    if let Some(requests) = expect.get("requests") {
        let requests = requests
            .as_array()
            .unwrap_or_else(|| panic!("case {}: expect.requests must be an array", case.id));
        let actual = recorded.lock().expect("not poisoned").clone();
        assert_requests(case, &actual, requests)?;
    }
    Ok(())
}

/// Builds the gateway the case asks for, runs its operation, and records the
/// requests that reached it.
async fn dispatch(case: &Case, recorded: &Arc<Mutex<Vec<Recorded>>>) -> Result<Value, OpError> {
    let spec = GatewaySpec::parse(case);
    if is_injected(case) {
        let transport = Injected::new(&spec, Arc::clone(recorded));
        let builder = client_builder(
            ClientBuilder::with_transport("http://gateway.invalid", transport),
            case,
        );
        return run_op(builder, case).await;
    }
    if spec.unreachable {
        let builder = client_builder(ClientBuilder::new("http://127.0.0.1:1"), case);
        return run_op(builder, case).await;
    }

    let server = start_gateway(&spec).await;
    let builder = client_builder(ClientBuilder::new(server.uri()), case);
    let outcome = run_op(builder, case).await;
    // The server records bodies as bytes; every asserted body is JSON.
    *recorded.lock().expect("not poisoned") = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|request| {
            let path = match request.url.query() {
                Some(query) => format!("{}?{query}", request.url.path()),
                None => request.url.path().to_owned(),
            };
            Recorded {
                method: request.method.as_str().to_owned(),
                path,
                headers: request
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_owned(),
                            value.to_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect(),
                body: json_or_null(&request.body),
            }
        })
        .collect();
    outcome
}

/// The operation table: one arm per `conformance/features.json` operation.
#[expect(
    clippy::too_many_lines,
    reason = "the table mirrors the vector operations one-for-one"
)]
async fn run_op<T: Transport>(builder: ClientBuilder<T>, case: &Case) -> Result<Value, OpError> {
    match case.operation.as_str() {
        "parse_claim_ref" => {
            let parsed = parse_claim_ref(input_str(case, "ref")).map_err(OpError::ClaimRef)?;
            Ok(json!({
                "tenant_id": parsed.tenant_id(),
                "claim_id": parsed.claim_id(),
                "path": parsed.path(),
            }))
        }
        "parse_headers" => {
            let headers = input_headers(case);
            let parsed = parse_headers(&headers).map_err(OpError::Hook)?;
            Ok(json!({
                "id": parsed.id,
                "source": parsed.source,
                "tenant": parsed.tenant,
                "content_type": parsed.content_type,
                "dedupe_key": parsed.dedupe_key,
                "replay_id": parsed.replay_id,
                "idempotency_key": parsed.idempotency_key,
            }))
        }
        "decode_message" => {
            let message =
                decode_message(input_str(case, "message").as_bytes()).map_err(OpError::Message)?;
            Ok(message_json(&message))
        }
        "idempotency_key" => {
            let include_replay = case
                .input
                .get("include_replay")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let key = match case.input.get("message").and_then(Value::as_str) {
                Some(message) => decode_message(message.as_bytes())
                    .map_err(OpError::Message)?
                    .idempotency_key(include_replay),
                None => parse_headers(&input_headers(case))
                    .map_err(OpError::Hook)?
                    .idempotency_key(include_replay),
            };
            Ok(json!({ "key": key }))
        }
        "redeem" => {
            let client = claim_check(builder);
            let bytes = client
                .redeem(input_str(case, "ref"), input_str(case, "sha256"))
                .await
                .map_err(OpError::Claim)?;
            Ok(json!({ "body": { "base64": BASE64.encode(&bytes) } }))
        }
        "health" => {
            let client = claim_check(builder);
            Ok(to_value(&client.health().await.map_err(OpError::Claim)?))
        }
        "routes_health" => {
            let client = routes(builder);
            Ok(to_value(&client.health().await.map_err(OpError::Routes)?))
        }
        "routes_list" => {
            let client = routes(builder);
            let params: ListRoutesParams = input_or_default(case, "params");
            Ok(to_value(
                &client.list_routes(&params).await.map_err(OpError::Routes)?,
            ))
        }
        "routes_create" => {
            let client = routes(builder);
            let input: RouteInput = input_required(case, "input");
            Ok(to_value(
                &client.create_route(&input).await.map_err(OpError::Routes)?,
            ))
        }
        "routes_get" => {
            let client = routes(builder);
            Ok(to_value(
                &client
                    .get_route(input_str(case, "id"))
                    .await
                    .map_err(OpError::Routes)?,
            ))
        }
        "routes_replace" => {
            let client = routes(builder);
            let input: RouteInput = input_required(case, "input");
            Ok(to_value(
                &client
                    .replace_route(input_str(case, "id"), &input)
                    .await
                    .map_err(OpError::Routes)?,
            ))
        }
        "routes_update" => {
            let client = routes(builder);
            let patch: RoutePatch = input_required(case, "patch");
            Ok(to_value(
                &client
                    .update_route(input_str(case, "id"), &patch)
                    .await
                    .map_err(OpError::Routes)?,
            ))
        }
        "routes_delete" => {
            let client = routes(builder);
            client
                .delete_route(input_str(case, "id"))
                .await
                .map_err(OpError::Routes)?;
            Ok(Value::Null)
        }
        "routes_ip_rules_get" => {
            let client = routes(builder);
            Ok(to_value(
                &client.get_ip_rules().await.map_err(OpError::Routes)?,
            ))
        }
        "routes_ip_rules_put" => {
            let client = routes(builder);
            let rules: IpRules = input_required(case, "rules");
            Ok(to_value(
                &client.put_ip_rules(&rules).await.map_err(OpError::Routes)?,
            ))
        }
        "routes_test" => {
            let client = routes(builder);
            let request: DryRunRequest = input_required(case, "request");
            Ok(to_value(
                &client.test_route(&request).await.map_err(OpError::Routes)?,
            ))
        }
        "admin_health" => {
            let client = admin(builder);
            Ok(to_value(&client.health().await.map_err(OpError::Admin)?))
        }
        "admin_metrics" => {
            let client = admin(builder);
            Ok(json!({ "text": client.metrics().await.map_err(OpError::Admin)? }))
        }
        "admin_config" => {
            let client = admin(builder);
            Ok(to_value(&client.config().await.map_err(OpError::Admin)?))
        }
        "admin_dlq_list" => {
            let client = admin(builder);
            let params: ListDeadLettersParams = input_or_default(case, "params");
            Ok(to_value(
                &client
                    .list_dead_letters(&params)
                    .await
                    .map_err(OpError::Admin)?,
            ))
        }
        "admin_replay_create" => {
            let client = admin(builder);
            let spec: ReplaySpec = input_required(case, "spec");
            Ok(to_value(
                &client.create_replay(&spec).await.map_err(OpError::Admin)?,
            ))
        }
        "admin_replay_get" => {
            let client = admin(builder);
            Ok(to_value(
                &client
                    .get_replay(input_str(case, "id"))
                    .await
                    .map_err(OpError::Admin)?,
            ))
        }
        "admin_replay_list" => {
            let client = admin(builder);
            Ok(to_value(
                &client.list_replays().await.map_err(OpError::Admin)?,
            ))
        }
        "admin_replay_update" => {
            let client = admin(builder);
            let patch: ReplayPatch = input_required(case, "patch");
            Ok(to_value(
                &client
                    .update_replay(input_str(case, "id"), &patch)
                    .await
                    .map_err(OpError::Admin)?,
            ))
        }
        "admin_quarantine" => {
            let client = admin(builder);
            let params: ListQuarantinedParams = input_or_default(case, "params");
            Ok(to_value(
                &client
                    .list_quarantined(&params)
                    .await
                    .map_err(OpError::Admin)?,
            ))
        }
        other => panic!("case {}: unknown operation {other:?}", case.id),
    }
}

/// An error an operation can raise, mapped to the class the vectors name.
enum OpError {
    ClaimRef(
        #[expect(
            dead_code,
            reason = "the vectors pin the class, not the message; the payload is kept so the variant wraps the error it stands for"
        )]
        InvalidClaimRefError,
    ),
    Hook(MissingHookIdError),
    Message(InvalidMessageError),
    Claim(ClaimCheckError),
    Routes(RoutesError),
    Admin(AdminError),
}

fn error_json(err: &OpError) -> Value {
    let mut map = Map::new();
    match err {
        OpError::ClaimRef(_) => {
            // No is_retryable() on a parse error: it is never retryable.
            entry(&mut map, "InvalidClaimRefError", false);
        }
        OpError::Hook(_) => {
            map.insert("class".to_owned(), json!("MissingHookIdError"));
        }
        OpError::Message(err) => {
            entry(&mut map, "InvalidMessageError", err.is_retryable());
            map.insert("code".to_owned(), json!(err.code));
            map.insert("field".to_owned(), json!(err.field));
        }
        // Every class reports the SDK's own `is_retryable()`: a literal would
        // hide a wrong implementation behind a passing vector.
        OpError::Claim(err) => {
            let retryable = err.is_retryable();
            match err {
                ClaimCheckError::InvalidRef(_) | ClaimCheckError::InvalidSha256 { .. } => {
                    entry(&mut map, "InvalidClaimRefError", retryable);
                }
                ClaimCheckError::NotFound => entry(&mut map, "ClaimNotFoundError", retryable),
                ClaimCheckError::Rejected { status, body } => {
                    entry(&mut map, "ClaimRejectedError", retryable);
                    map.insert("status".to_owned(), json!(status.as_u16()));
                    map.insert("body".to_owned(), body.clone());
                }
                ClaimCheckError::Integrity { .. } => {
                    entry(&mut map, "ClaimIntegrityError", retryable);
                }
                ClaimCheckError::Unavailable(_) => {
                    entry(&mut map, "ClaimCheckUnavailableError", retryable);
                }
                other => panic!("unmapped claim-check error {other:?}"),
            }
        }
        OpError::Routes(err) => {
            let retryable = err.is_retryable();
            match err {
                RoutesError::InvalidRouteId { .. } => {
                    entry(&mut map, "InvalidRouteIdError", retryable);
                }
                RoutesError::NotFound => entry(&mut map, "RouteNotFoundError", retryable),
                RoutesError::Rejected(rejection) => {
                    entry(&mut map, "RoutesRejectedError", retryable);
                    map.insert("status".to_owned(), json!(rejection.status.as_u16()));
                    map.insert("code".to_owned(), json!(rejection.code));
                    map.insert("field".to_owned(), json!(rejection.field));
                    map.insert("message".to_owned(), json!(rejection.message));
                    map.insert("conflicting_id".to_owned(), json!(rejection.conflicting_id));
                    map.insert("max_routes".to_owned(), json!(rejection.max_routes));
                }
                RoutesError::Unavailable(_) => {
                    entry(&mut map, "RoutesUnavailableError", retryable);
                }
                other => panic!("unmapped routes error {other:?}"),
            }
        }
        OpError::Admin(err) => {
            let retryable = err.is_retryable();
            match err {
                AdminError::RoleNotEnabled { role } => {
                    entry(&mut map, "RoleNotEnabledError", retryable);
                    map.insert("role".to_owned(), json!(role));
                }
                AdminError::Rejected { status, code } => {
                    entry(&mut map, "AdminRejectedError", retryable);
                    map.insert("status".to_owned(), json!(status.as_u16()));
                    map.insert("code".to_owned(), json!(code));
                }
                AdminError::Unavailable(_) => {
                    entry(&mut map, "AdminUnavailableError", retryable);
                }
                other => panic!("unmapped admin error {other:?}"),
            }
        }
    }
    Value::Object(map)
}

fn entry(map: &mut Map<String, Value>, class: &str, retryable: bool) {
    map.insert("class".to_owned(), json!(class));
    map.insert("retryable".to_owned(), json!(retryable));
}

fn assert_requests(case: &Case, actual: &[Recorded], expected: &[Value]) -> Result<(), Failed> {
    if actual.len() != expected.len() {
        return Err(Failed::from(format!(
            "case {}: expected {} requests, got {}: {actual:?}",
            case.id,
            expected.len(),
            actual.len()
        )));
    }
    for (index, (got, want)) in actual.iter().zip(expected).enumerate() {
        let method = want.get("method").and_then(Value::as_str);
        if method != Some(got.method.as_str()) {
            return Err(Failed::from(format!(
                "case {}: request {index} method: expected {}, got {}",
                case.id,
                method.unwrap_or("(absent)"),
                got.method
            )));
        }
        let path = want.get("path").and_then(Value::as_str);
        if path != Some(got.path.as_str()) {
            return Err(Failed::from(format!(
                "case {}: request {index} path: expected {}, got {}",
                case.id,
                path.unwrap_or("(absent)"),
                got.path
            )));
        }
        if let Some(headers) = want.get("headers").and_then(Value::as_object) {
            for (name, value) in headers {
                if got.headers.get(name).map(String::as_str) != value.as_str() {
                    return Err(Failed::from(format!(
                        "case {}: request {index} header {name}: expected {value}, got {}",
                        case.id,
                        got.headers.get(name).map_or("(absent)", String::as_str)
                    )));
                }
            }
        }
        // An absent `body` means "not asserted": not every vector pins one.
        if let Some(body) = want.get("body") {
            if got.body != *body {
                return Err(Failed::from(format!(
                    "case {}: request {index} body: expected {body}, got {}",
                    case.id, got.body
                )));
            }
        }
    }
    Ok(())
}

/// Bytes cannot round-trip through JSON: both sides become base64.
fn expected_ok(case: &Case, expected: &Value) -> Value {
    if case.operation != "redeem" {
        return expected.clone();
    }
    let bytes = body_bytes(expected.get("body"));
    json!({ "body": { "base64": BASE64.encode(&bytes) } })
}

fn is_injected(case: &Case) -> bool {
    case.input
        .get("client")
        .and_then(|client| client.get("transport"))
        .and_then(Value::as_str)
        == Some("injected")
}

fn client_builder<T: Transport>(mut builder: ClientBuilder<T>, case: &Case) -> ClientBuilder<T> {
    let Some(client) = case.input.get("client").and_then(Value::as_object) else {
        return builder;
    };
    if let Some(headers) = client.get("headers").and_then(Value::as_object) {
        for (name, value) in headers {
            if let Some(value) = value.as_str() {
                builder = builder.header(name, value);
            }
        }
    }
    if let Some(millis) = client.get("timeout_ms").and_then(Value::as_u64) {
        builder = builder.timeout(Duration::from_millis(millis));
    }
    builder
}

fn claim_check<T: Transport>(builder: ClientBuilder<T>) -> ClaimCheckClient<T> {
    builder.claim_check().expect("conformance client config")
}

fn routes<T: Transport>(builder: ClientBuilder<T>) -> RoutesClient<T> {
    builder.routes().expect("conformance client config")
}

fn admin<T: Transport>(builder: ClientBuilder<T>) -> AdminClient<T> {
    builder.admin().expect("conformance client config")
}

fn input_str<'a>(case: &'a Case, key: &str) -> &'a str {
    case.input
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("case {}: input.{key} must be a string", case.id))
}

fn input_required<T: DeserializeOwned>(case: &Case, key: &str) -> T {
    let value = case
        .input
        .get(key)
        .unwrap_or_else(|| panic!("case {}: missing input.{key}", case.id));
    serde_json::from_value(value.clone())
        .unwrap_or_else(|err| panic!("case {}: input.{key}: {err}", case.id))
}

fn input_or_default<T: DeserializeOwned + Default>(case: &Case, key: &str) -> T {
    match case.input.get(key) {
        Some(value) => serde_json::from_value(value.clone())
            .unwrap_or_else(|err| panic!("case {}: input.{key}: {err}", case.id)),
        None => T::default(),
    }
}

/// Builds request headers. `HeaderName::from_bytes` lowercases, so the lookups
/// behave exactly as a server's would.
fn input_headers(case: &Case) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let Some(given) = case.input.get("headers").and_then(Value::as_object) else {
        return headers;
    };
    for (name, value) in given {
        let header = HeaderName::from_bytes(name.as_bytes())
            .unwrap_or_else(|err| panic!("case {}: header name {name:?}: {err}", case.id));
        let value = value
            .as_str()
            .unwrap_or_else(|| panic!("case {}: header {name:?} must be a string", case.id));
        let value = HeaderValue::from_str(value)
            .unwrap_or_else(|err| panic!("case {}: header {name:?}: {err}", case.id));
        headers.insert(header, value);
    }
    headers
}

fn to_value<T: serde::Serialize>(value: T) -> Value {
    serde_json::to_value(value).expect("serializable")
}

/// The decoded message, with the inline body re-encoded as standard base64 so
/// both sides compare bytes.
fn message_json(message: &Message) -> Value {
    json!({
        "v": message.v,
        "id": message.id,
        "source_id": message.source_id,
        "tenant_id": message.tenant_id,
        "received_at": message.received_at,
        "content_type": message.content_type,
        "size": message.size,
        "body_base64": message.body.as_ref().map(|body| BASE64.encode(body)),
        "claim": message.claim,
        "sha256": message.sha256,
        "dedupe_key": message.dedupe_key,
        "replay_id": message.replay_id,
        "idempotency_key": message.idempotency_key,
        "headers": message.headers,
    })
}

fn json_or_null(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

/// The gateway a case asks for.
struct GatewaySpec {
    unreachable: bool,
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Bytes,
    delay: Option<Duration>,
}

impl GatewaySpec {
    fn parse(case: &Case) -> Self {
        let spec = case.input.get("gateway").cloned().unwrap_or(Value::Null);
        let status = spec
            .get("status")
            .and_then(Value::as_u64)
            .map_or(StatusCode::OK, |status| {
                StatusCode::from_u16(u16::try_from(status).expect("status fits u16"))
                    .expect("valid status")
            });
        let headers = spec
            .get("headers")
            .and_then(Value::as_object)
            .map(|headers| {
                headers
                    .iter()
                    .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            unreachable: spec
                .get("unreachable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            status,
            headers,
            body: body_bytes(spec.get("body")),
            delay: spec
                .get("delay_ms")
                .and_then(Value::as_u64)
                .map(Duration::from_millis),
        }
    }
}

/// The three body shapes the vectors use.
fn body_bytes(body: Option<&Value>) -> Bytes {
    let Some(Value::Object(body)) = body else {
        return Bytes::new();
    };
    if let Some(text) = body.get("text").and_then(Value::as_str) {
        return Bytes::copy_from_slice(text.as_bytes());
    }
    if let Some(encoded) = body.get("base64").and_then(Value::as_str) {
        return Bytes::from(BASE64.decode(encoded).expect("valid base64"));
    }
    if let Some(value) = body.get("json") {
        return Bytes::from(serde_json::to_vec(value).expect("serializable"));
    }
    panic!("unknown gateway body {body:?}");
}

/// A real HTTP server standing in for a deployment's gateway: it answers every
/// request with the spec and records what it saw.
async fn start_gateway(spec: &GatewaySpec) -> MockServer {
    let server = MockServer::start().await;
    let mut template =
        ResponseTemplate::new(spec.status.as_u16()).set_body_bytes(spec.body.to_vec());
    for (name, value) in &spec.headers {
        template = template.insert_header(name.as_str(), value.as_str());
    }
    // A real delay: the `*.client.timeout` vectors need a response that
    // outlives the client's deadline.
    if let Some(delay) = spec.delay {
        template = template.set_delay(delay);
    }
    Mock::given(any())
        .respond_with(template)
        .mount(&server)
        .await;
    server
}

/// The injected-transport hook: serves the spec in-process and records
/// requests, with no server at all.
#[derive(Debug)]
struct Injected {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Bytes,
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl Injected {
    fn new(spec: &GatewaySpec, recorded: Arc<Mutex<Vec<Recorded>>>) -> Self {
        Self {
            status: spec.status,
            headers: spec.headers.clone(),
            body: spec.body.clone(),
            recorded,
        }
    }
}

impl Transport for Injected {
    fn send(
        &self,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>, TransportError>> + Send {
        let status = self.status;
        let headers = self.headers.clone();
        let body = self.body.clone();
        let recorded = Arc::clone(&self.recorded);
        // `path_and_query` is the request target as sent, query string
        // included: what a real server would record.
        let path = request
            .uri()
            .path_and_query()
            .map_or_else(String::new, ToString::to_string);
        let method = request.method().as_str().to_owned();
        let request_headers: BTreeMap<String, String> = request
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let request_body = json_or_null(request.body());
        async move {
            recorded.lock().expect("not poisoned").push(Recorded {
                method,
                path,
                headers: request_headers,
                body: request_body,
            });
            let mut builder = Response::builder().status(status);
            for (name, value) in &headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            builder.body(body).map_err(TransportError::new)
        }
    }
}
