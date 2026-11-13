# ankusa_gateway

The Ankusa gateway: an HTTP listener that serves claim-check refs to queue
workers, and authenticates and authorizes every request with a configurable
chain. It runs beside an `ankusa` core instance (which owns the claim stores),
as its own image (`jamescarr/ankusa-gateway`), inside `jamescarr/ankusa` with
the `claim_check` role, or embedded in your own Elixir application.

Design and wire formats: [ADR 0005](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0005-standalone-gateway.md).
The claims API contract: `priv/openapi/claim_check.v1.yaml`.

## Embedding

```elixir
# mix.exs
{:ankusa, "~> 0.8"},
{:ankusa_gateway, "~> 0.1"}
```

```elixir
children = [
  {Ankusa.Instance,
   Ankusa.Config.new(
     instance: :default,
     # no role: a gateway-only node. Keep yours if this node also ingests.
     roles: [],
     claim_check: %{blob_stores: [{Ankusa.BlobStore.S3, bucket: "claims", region: "us-east-1"}]},
     metrics: %{enabled: true, extra: [Ankusa.Gateway.Metrics]}
   )},
  {Ankusa.Gateway,
   Ankusa.Gateway.Config.new(
     instance: :default,
     port: 4001,
     ip: "0.0.0.0",
     auth: %{
       authenticators: [
         %{type: :jwt, issuer: "https://login.example.com/", audience: "ankusa",
           jwks_url: "https://login.example.com/.well-known/jwks.json"}
       ]
     }
   )}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Start the instance first: `Ankusa.Gateway` refuses to start without it.

## Moving from core's `:claim_check` role

| Core, before | Here |
| --- | --- |
| `roles: [:claim_check]` | `roles: []` (or your other roles) plus `{Ankusa.Gateway, config}` |
| `claim_check.port`, `ip`, `tls` | `Ankusa.Gateway.Config` `port`, `ip`, `tls` |
| `claim_check.auth` (`mode` + `tokens` / `jwt` / `mtls`) | `auth.authenticators`: one entry of `type: :token`, `:jwt` or `:mtls` carrying the same block; `mode: :none` is `[]` |
| `claim_check.authorize: {m, f, a}` | a module implementing `Ankusa.Gateway.Authorizer`, in `authz.authorizers`, named by the `claims` policy |
| `claim_check.max_concurrent_reads` | `max_concurrent_requests` |
| `claim_check.integrity_check` | `claims.integrity_check` |

Every other `claim_check.*` key (stores, placement, key layout, ref signing,
`read_*`) stays in core's config. The YAML images migrate a version-1 file
for you.

## Authentication

`auth.authenticators` is tried in order. An authenticator that finds no
credential of its form steps aside; one whose form it is accepts or refuses
(`401`). An identity provider that does not answer is `503 auth_unavailable`.

* `token` — static bearer tokens, by SHA-256: `%{id, sha256, scopes, groups, claims}`.
* `jwt` — an issuer's access tokens. Several `jwt` authenticators can share a
  chain, one per issuer (a token is routed by its unverified `iss`). Two
  issuers that can both name an `alice` are two principals only when their
  `subject_prefix` differs: the principal id is what authorizers see.
* `mtls` — client certificates, mapped by identity to scopes and groups
  (needs `tls` with `verify: :verify_peer`).

An empty chain is accepted on loopback only, or with
`auth.allow_insecure: true` when something in front of the listener
authenticates every caller.

## Authorization

A policy is a list of authorizer steps and a mode (`:first`, `:all`, `:any`).
The claims API's default policy is `[scopes]`: `claims:read` and the claim's
tenant (`tenant:<id>` or `tenant:*`). Replace or extend it:

```elixir
authz: %{
  authorizers: %{"mine" => {MyApp.ClaimPolicy, %{}}},
  policies: %{"claims" => %{mode: :first, steps: ["mine", "scopes"]}}
}
```

A step that cannot decide fails closed (`503 authz_unavailable`) unless its
`on_unavailable` is `:abstain` or `:deny`. The whole policy has
`authz.deadline_ms` (250 ms).

`Ankusa.Gateway.Authz.Rules` decides from the token alone: ordered rules over
actions, spaces, key patterns (`teams/{team}/**`) and the principal's groups,
scopes, tenants and claims, the first match wins:

```elixir
authorizers: %{
  "teams" =>
    {Ankusa.Gateway.Authz.Rules,
     %{rules: [%{effect: :allow, actions: [:read], spaces: ["claims"],
                 keys: ["{tenant}/**"], when: %{groups_any: ["tenant-{{ vars.tenant }}"]}}]}}
}
```

An authorizer's `kind/1` says where it runs: `:inline` in the request,
`:cached` behind the decision cache, `:remote` behind the cache and one
`Ankusa.Gateway.Caller` per backend (coalesced, bounded, batched, behind a
breaker). Authentication is cached too, under the credential's SHA-256, never
past its expiry. Both caches are bounded (`auth.cache`, `authz.cache`).

## The objects API

A **space** (`spaces`) is an object store, a prefix in it and a policy: the
gateway then serves `GET`/`HEAD`/`PUT`/`DELETE` on
`/v1/objects/{space}/{key}` and one page of a listing on
`/v1/objects/{space}?prefix=&delimiter=&cursor=&limit=`, each authorized by
the space's policy. One space and one policy over it:

```elixir
Ankusa.Gateway.Config.new(
  instance: :default,
  authz: %{
    authorizers: %{
      "teams" =>
        {Ankusa.Gateway.Authz.Rules,
         %{
           rules: [
             %{effect: :allow, actions: [:read, :list], spaces: ["docs"],
               keys: ["teams/{team}/**"], when: %{groups_any: ["team-{{ vars.team }}"]}}
           ]
         }}
    },
    policies: %{"docs" => %{mode: :first, steps: ["teams"]}}
  },
  spaces: %{
    "docs" => %{
      store: {Ankusa.BlobStore.S3, bucket: "reports", region: "us-east-1"},
      prefix: "docs/",
      policy: "docs",
      max_object_bytes: 16_777_216
    }
  },
  objects: %{max_inflight_bytes: 536_870_912}
)
```

* Keys: valid UTF-8, 1 to 1024 bytes, at most 32 segments, none empty, `.` or
  `..`, none starting with `.ankusa`; a path's segments are percent-decoded
  once and a decoded `/` or `\` is refused. `my%20file.txt` and
  `?key=my+file.txt` name one object.
* Sizes: `PUT` needs `content-length` and is refused past the space's
  `max_object_bytes` (`413`); a `GET` that would send more is `422
  object_too_large`. Buffered bytes are capped by
  `objects.max_inflight_bytes` (`503 overloaded` past it).
* One `bytes` range (`206`, `416`), `If-None-Match` (`304`) and, on `PUT`,
  `if-none-match: *` / `if-match: "<version>"` (`412`) are the conditions.
  Metadata is `x-ankusa-meta-<name>` in and out.
* `spaces.<name>.read_only` answers `405` on `PUT` and `DELETE`.
* Store scope: an `s3`, `gcs` or LocalFS store; two spaces on one store may
  not have overlapping prefixes.

Wire format: `priv/openapi/gateway.v1.yaml`. The operator's view, with the
statuses and headers: the
[objects API section](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md#the-objects-api).

## Sharing

`sharing.enabled` turns on native grants, ownership and share links for the
spaces. It defines the `grants` authorizer (a policy must list it), appends the
`link` authenticator, and makes `PUT` record `ankusa-owner` and
`ankusa-incarnation` on a new object:

```elixir
Ankusa.Gateway.Config.new(
  instance: :default,
  authz: %{
    authorizers: %{
      "team" =>
        {Ankusa.Gateway.Authz.Rules,
         %{
           rules: [
             %{effect: :allow, actions: [:read, :write, :share], spaces: ["docs"],
               keys: ["teams/{team}/**"], when: %{groups_any: ["{{ vars.team }}"]}}
           ]
         }}
    },
    policies: %{"docs" => %{mode: :first, steps: ["grants", "team"]}}
  },
  spaces: %{
    "docs" => %{store: {Ankusa.BlobStore.S3, bucket: "reports", region: "us-east-1"},
                prefix: "docs/", policy: "docs"}
  },
  sharing: %{
    # [current | previous…], each at least 16 bytes; the first signs a link,
    # any verifies one (rotation = put the new key first).
    signing_keys: [System.get_env("LINK_SIGNING_KEY")],
    link_max_ttl_s: 604_800,
    acl_cache_ttl_ms: 2_000,
    max_grants_per_resource: 256
  }
)
```

Then, over `/v1/spaces/{space}`: `GET …/acl?key=` (`?prefix=`), `POST …/grants`,
`DELETE …/grants/{id}?key=`, `POST …/links` (a `link` grant plus a signed URL).
Every route authorizes `share` on the resource, and a new grant needs each of
its own actions on it too. Each mutation writes an audit record v1 under
`.ankusa/audit/v1/` first.

A share link is a credential for `GET` and `HEAD` of that one object and
nothing else, and only the `grants` step ever sees it. Its grant's revocation
is bounded by `acl_cache_ttl_ms` on other nodes. On a LocalFS space grant
updates are only safe from one gateway node (the compare-and-swap lock is
`:global`, not the filesystem's); use an object store whose conditions are.

## Metrics

Add `Ankusa.Gateway.Metrics` to core's `metrics.extra`:
`ankusa_gateway_requests_total{api,status}`,
`ankusa_gateway_request_duration_seconds{api}`, the authentication and policy
decisions (`ankusa_gateway_authn_total`, `ankusa_gateway_authz_decisions_total`,
`ankusa_gateway_authz_duration_seconds`), each remote backend's caller
(`ankusa_gateway_calls_total`, `ankusa_gateway_caller_{limit,inflight,queued}`,
`ankusa_gateway_caller_breaker_transitions_total`) and the caches
(`ankusa_gateway_cache_{entries,bytes}`); with sharing on,
`ankusa_gateway_shares_total{action,result}`.
