---
title: "Eight SDKs, one conformance suite: how I kept TypeScript, Python, Rust, Ruby, Go, PHP, Elixir, and Java honest"
date: 2026-11-10
slug: eight-sdks-one-conformance-suite
description: "Eight consumer SDKs for Ankusa share one set of language-neutral test vectors, so none of them can drift. What the suite checks and what the SDKs leave out."
tags: [webhooks, elixir, sdk, conformance, polyglot]
draft: true
---

Eight SDKs written by one person in eight languages will drift: one treats an empty header as absent, another as an empty string, and nobody notices until a worker in production disagrees with its neighbour. So I did not write SDK tests per language. I wrote one suite that every SDK has to pass.

This post is about that suite, what it makes the SDKs do, and what I deliberately kept out of them. If you have not read the launch post, it is at {{BLOG_URL}}/ankusa-launch. Everything below is 0.x and APIs may change.

## Why consumer SDKs at all

Ankusa sits in front of your worker. By the time a delivery reaches you, the edge has verified it and durably accepted it, and it arrives over HTTP or a broker (RabbitMQ, Kafka, NATS, Redis). An Ankusa worker needs exactly five things from a library:

1. Parse the `x-ankusa-*` headers on an HTTP delivery.
2. Decode the v1 queue message from a broker, with the size and sha256 integrity checks.
3. Read the idempotency key, because delivery is at-least-once and receivers must be idempotent on `x-ankusa-idempotency-key`.
4. Redeem a claim-check reference with the sha256 check, for bodies that did not fit in the message.
5. Say whether an error is worth retrying.

The SDKs also drive the admin and routes APIs, which is how you manage routes from code instead of curl.

None of these is signature verification. The SDKs do not verify provider signatures, because Ankusa did that at the edge before it accepted anything. They also ship no broker client. You already have a Kafka or RabbitMQ client, and the SDK takes the message body that client hands you. I would rather have a small library that does five things identically in every language than a large one that does twenty things differently.

## The contract

The suite lives in `conformance/`. It has three parts: [`features.json`](https://github.com/jamescarr/ankusa/blob/main/conformance/features.json), the registry of features and operations; `cases/*.json`, the test vectors; and `check.mjs`, a stdlib-only Node script that validates both and then runs every registered SDK's native runner. The full format is in [conformance/README.md](https://github.com/jamescarr/ankusa/blob/main/conformance/README.md).

Today there are 131 cases across 7 files: `admin` 15, `claim_ref` 21, `health` 7, `message` 31, `redeem` 25, `routes` 25 and `webhook` 7. Each case names one feature and one operation, an input, and either an expected result or an expected error. Errors are compared by class name, and optionally by which requests the SDK sent, in order, with method, path and a subset of headers.

The registry has 22 feature ids:

`claim_ref.parse`, `claim_check.redeem`, `claim_check.redeem.input_validation`, `claim_check.redeem.integrity`, `claim_check.redeem.error_classification`, `claim_check.health`, `claim_check.client.headers`, `claim_check.client.timeout`, `claim_check.client.transport`, `webhook.parse_headers`, `message.decode`, `message.integrity`, `message.idempotency_key`, `routes.crud`, `routes.ip_rules`, `routes.dry_run`, `routes.errors`, `routes.input_validation`, `routes.request_shape`, `routes.health`, `admin.operations`, `admin.errors`.

And 25 operations:

`parse_claim_ref`, `redeem`, `health`, `parse_headers`, `routes_health`, `routes_list`, `routes_create`, `routes_get`, `routes_replace`, `routes_update`, `routes_delete`, `routes_ip_rules_get`, `routes_ip_rules_put`, `routes_test`, `admin_health`, `admin_metrics`, `admin_config`, `admin_dlq_list`, `admin_replay_create`, `admin_replay_get`, `admin_replay_list`, `admin_replay_update`, `admin_quarantine`, `decode_message`, `idempotency_key`.

The part that matters more than the counts is the runner contract. Every SDK ships a native runner that:

1. Loads every case and registers one test per case, named by its `id`.
2. Never filters or skips cases.
3. Fails on an unknown operation.
4. Imports only from the package's public entry point.
5. Maps errors by exact exported class name.
6. Is registered in `sdks.json`.

Rules 2 and 3 are what make it honest. When I add a feature to `features.json` with at least one case, every SDK fails until it implements the feature. A runner cannot quietly ignore an operation it does not understand, and a runner cannot pass by testing internals, because rule 4 forces it through the same door your code uses. Rule 5 is strict on purpose: `type(err) is cls` in Python, `err.constructor === cls` in TypeScript, no subclass matching. If the vector says `ClaimNotFoundError`, a more general class does not count.

Message decoding is where this pays off most. The decode rules run in a fixed order and the first failure wins, so the same malformed message produces the same error code in all eight languages. Bad JSON, wrong version, a field of the wrong type, both `body_base64` and `claim` present, a length that does not match `size`, a sha256 that does not match the body, a claim whose tenant disagrees with the message's `tenant_id`: the decode rules in the README cover each, and a poison message dead-letters the same way whichever language your worker is written in.

## The same five lines in eight languages

Each snippet below is copied from that SDK's README. They redeem a claim and branch on whether the error is retryable. The one-bit answer is the point: your worker needs to decide between requeue and dead-letter, and nothing else about the failure.

TypeScript:

```ts
  try {
    return await claimCheck.redeem(ref, sha256);
  } catch (err) {
    if (err instanceof ClaimCheckError && !err.retryable) {
      // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
      throw err;
    }
```

Python:

```python
    try:
        return claim_check.redeem(message["claim"], message["sha256"])
    except ClaimCheckError as err:
        if not err.retryable:
            # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
            raise
```

Rust:

```rust
let client = ClaimCheckClient::new("http://127.0.0.1:4001")?;
match client
    .redeem("urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002", "8e1bed597394cf01672e49232d9929c8bc6b3d6ea4e2f489517ec88701f01581")
    .await
{
    Err(err) if err.is_retryable() => eprintln!("retry later: {err}"),
    Err(err) => eprintln!("give up: {err}"),
    Ok(bytes) => println!("{} bytes", bytes.len()),
}
```

Ruby:

```ruby
def resolve_body(message)
  CLAIM_CHECK.redeem(message["claim"], message["sha256"])
rescue Ankusa::ClaimCheckError => e
  # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
  raise unless e.retryable?
```

Go:

```go
body, err := claimCheck.Redeem(ctx, ref, sha256)
if err != nil {
    var apiErr ankusa.Error
    if errors.As(err, &apiErr) && !apiErr.Retryable() {
        // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
    }
    return err
}
```

PHP:

```php
    try {
        return $claimCheck->redeem($message['claim'], $message['sha256']);
    } catch (ClaimCheckError $err) {
        if (! $err->isRetryable()) {
            // bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
            throw $err;
        }
```

Elixir:

```elixir
case Ankusa.SDK.ClaimCheck.redeem(claim_check, claim, sha256) do
  {:ok, body} ->
    handle(body)

  {:error, %{retryable: false} = error} ->
    # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
    dead_letter(error)

  {:error, %{retryable: true} = error} ->
    # gateway unreachable or 5xx: safe to retry
    requeue(error)
end
```

Java:

```java
  byte[] body = claimCheck.redeem(claim, sha256);
} catch (ClaimCheckError e) {
  if (!e.retryable()) {
    // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
  }
  throw e;
}
```

Java is in the repo and passes the suite; its first Maven Central release is pending. I will not post the Java announcement until it is out.

The shapes differ because the languages differ: an exception with a `retryable` attribute, a `retryable?` predicate, a typed error you unwrap with `errors.As`, a tagged tuple. The behaviour behind them is pinned by the same vectors. A 404 from the gateway is not retryable. A sha256 mismatch is not retryable. An unreachable gateway or a `503` is.

## What is language-specific

The suite pins behaviour, not API shape, so each SDK is free to be idiomatic. Here is where I took that freedom:

- **Elixir** is the only SDK with a framework piece. `Ankusa.SDK.Receiver` is a `Plug` that hands `Ankusa.Sink.Http` deliveries to a handler module of yours, so a Phoenix or Bandit app can receive them without writing the header parsing.
- **Rust** is async on Tokio, and the HTTP layer sits behind a `Transport` you can inject with `ClientBuilder::with_transport`. Tests run against an in-memory transport with no network. The conformance runner uses the same hook, which is what the suite's `"transport": "injected"` client option means.
- **PHP** speaks PSR-18. Guzzle is installed by default, and any other PSR-18 client can be injected instead.
- **Go** is stdlib only. `go.mod` has no `require` line.
- **Ruby** is stdlib only too; the gemspec declares no runtime dependency.
- **TypeScript** has types generated from `packages/ankusa/priv/openapi/claim_check.v1.yaml`, targets Node 20 or newer, and is ESM only. `parseHeaders` takes Express's `req.headers` directly.
- **Python** needs 3.11 or newer. `ClaimCheckClient` owns an `httpx.Client` and is a context manager.
- **Java** targets 17 and newer, uses `java.net.http.HttpClient`, and depends on Jackson 3.

The claim-check contract is an OpenAPI document, [`claim_check.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/claim_check.v1.yaml), and the TypeScript types come from it. But the queue-message decode rules, the idempotency-key rules and the error mapping are behaviour, and a schema does not express behaviour. The vectors do.

## Running it

`mise run check:conformance` is the gate. It checks that every `packages/sdk-*` directory is registered in `sdks.json` and that every registered entry exists, then runs each SDK's `setup` steps and its runner in order. The first non-zero exit fails that SDK. CI runs the same task as the `sdk conformance` job.

The failure mode I care about is the quiet one. A new operation that one SDK has not implemented must turn the build red, not leave a hole nobody notices until a user hits it. That is the reason for the "never skip, fail on unknown" rules, and it is the reason I trust the claim in this post's title.

## What the suite does not cover

The vectors test the SDKs against a mock gateway. They do not test a live Ankusa node, and they do not test your broker client, because the SDKs do not ship one. A passing suite says all eight SDKs agree with the vectors, not that the vectors are complete. The format is plain JSON, so a pull request with a failing vector is the best bug report I can get.

## Install

|Language|Install|
|---|---|
|TypeScript|`npm install ankusa`|
|Python|`pip install ankusa` (or `uv add ankusa`)|
|Rust|`cargo add ankusa`|
|Ruby|`gem install ankusa-sdk`|
|Go|`go get github.com/jamescarr/ankusa/packages/sdk-go`|
|PHP|`composer require jamescarr/ankusa`|
|Elixir (SDK)|`{:ankusa_sdk, "~> 0.3"}`|
|Java|`io.github.jamescarr:ankusa-sdk` (pending first release)|

Each SDK's README is under [packages/](https://github.com/jamescarr/ankusa/tree/main/packages) in the repo, for example the [TypeScript one](https://github.com/jamescarr/ankusa/blob/main/packages/sdk-typescript/README.md).

## Try it

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 -e ANKUSA_ADMIN_IP=0.0.0.0 \
  -v ankusa-data:/var/lib/ankusa \
  jamescarr/ankusa:edge

# the image ships a healthcheck: wait for it rather than racing the listener
until [ "$(docker inspect --format '{{.State.Health.Status}}' ankusa)" = healthy ]; do sleep 1; done

curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"accepted"}   (returned only after the store fsync)
```

The [quickstart](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md) walks through outages, dead letters, replay, and pointing a real provider at it. The code is at <https://github.com/jamescarr/ankusa>. It's 0.x; tell me what breaks: <https://github.com/jamescarr/ankusa/issues>
