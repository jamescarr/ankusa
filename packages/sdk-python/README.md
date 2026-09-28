# ankusa

The client SDK for [Ankusa](https://github.com/jamescarr/ankusa) deployments:
one PyPI package meant to bundle everything a non-Elixir consumer needs to
talk to an Ankusa deployment. Today that's the
[claim-check gateway](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)
client and a webhook-receiving header helper; more clients (ingest, admin)
land here as they're built.

## Install

Not published yet. Until the first release, depend on it as a local path,
the same way the Elixir packages in this monorepo depend on `ankusa` core
before their first Hex release, and `packages/sdk-typescript` depends on
itself via `file:`.

With [`uv`](https://docs.astral.sh/uv/):

```toml
[project]
dependencies = ["ankusa"]

[tool.uv.sources]
ankusa = { path = "../../packages/sdk-python" }
```

Or with plain `pip`:

```sh
pip install -e ../../packages/sdk-python
```

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the message's
`sha256`, and classify failures into dead-letter vs. retry,
without holding any object-store credentials. Conforms to the framework's
own contract, [`priv/openapi/claim_check.v1.yaml`](../ankusa/priv/openapi/claim_check.v1.yaml):
the spec is the source of truth, this package conforms to it, not the
reverse.

```python
import os

from ankusa import ClaimCheckClient, ClaimCheckError

claim_check = ClaimCheckClient(os.environ.get("CLAIM_CHECK_URL", "http://localhost:4001"))

# A queue message that carries a claim also carries its sha256:
#   "claim":  "urn:ankusa:claim:v1:<tenant>:<claim_id>"  (claim_id: uppercase ULID)
#   "sha256": 64-char lowercase hex of the claim's bytes
def resolve_body(message: dict) -> bytes:
    try:
        return claim_check.redeem(message["claim"], message["sha256"])
    except ClaimCheckError as err:
        if not err.retryable:
            # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
            raise
        # gateway unreachable or 5xx: safe to retry
        raise
```

`redeem()` does three things `GET /v1/claims/...` alone doesn't:

1. Parses the ref into its tenant id, claim id, and gateway path
   (`GET /v1/claims/{tenant_id}/{claim_id}`) (`parse_claim_ref`, also
   exported standalone).
2. Fetches the bytes.
3. Verifies them against the message's `sha256` (the gateway
   itself does not check this, see "Redeem a claim" in
   [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md))
   before ever returning them to you.

Every failure is a `ClaimCheckError` subclass with a `retryable` attribute,
so a consumer needs exactly one bit to decide dead-letter vs. retry:

| Class | `retryable` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `False` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64-char lowercase hex |
| `ClaimNotFoundError` | `False` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `False` | gateway `4xx` other than `404` (`.status`, `.body`) |
| `ClaimIntegrityError` | `False` | sha256 of the returned bytes doesn't match |
| `ClaimCheckUnavailableError` | `True` | gateway `5xx`/`503`, or unreachable |

`health()` hits `GET /health` for a liveness probe.

`ClaimCheckClient` owns an `httpx.Client`; use it as a context manager
(`with ClaimCheckClient(...) as claim_check:`) or call `.close()` yourself
when done.

## Webhook receiver helper

Every receiver of Ankusa's HTTP sink needs the same handful of headers off
each request; `parse_headers` replaces the hand-rolled `self.headers.get(...)`
calls with one call and a typed result:

```python
from ankusa import MissingHookIdError, parse_headers

def do_POST(self):
    try:
        hook = parse_headers(self.headers)
    except MissingHookIdError:
        self.send_response(400)
        self.end_headers()
        return

    body = self.rfile.read(int(self.headers.get("content-length", "0")))
    # hook.id, hook.source, hook.seq, hook.tenant, hook.content_type
    ...
```

`HookHeaders.id` is what a receiver dedupes on: delivery is at-least-once
(see "HTTP handoff" in
[`docs/integrations.md`](https://github.com/jamescarr/ankusa/blob/main/docs/integrations.md)),
so the same hook can arrive twice after a retry. Header lookup is always
case-insensitive, regardless of whether the mapping passed in already is.

[`examples/quickstart/worker.py`](https://github.com/jamescarr/ankusa/tree/main/examples/quickstart/worker.py)
uses this.

## Layout

```
src/ankusa/
  __init__.py           # umbrella barrel: re-exports every client this package bundles
  py.typed
  webhook.py             # x-ankusa-* header parsing for HTTP-sink receivers
  claim_check/           # the claim-check gateway client
    __init__.py          # barrel for this client
    client.py
    ref.py
    errors.py
tests/
  test_client.py
  test_ref.py
  test_webhook.py
```

A future client (say, an ingest helper) gets its own `src/ankusa/<name>/`
directory with the same shape, re-exported from `src/ankusa/__init__.py`.

## Develop

```sh
uv sync
uv run pytest
uv run mypy
```
