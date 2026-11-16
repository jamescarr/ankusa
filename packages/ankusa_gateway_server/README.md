# jamescarr/ankusa-gateway

The Ankusa gateway as its own image: the claims API that queue workers redeem
claim-check refs at (`GET /v1/claims/{tenant_id}/{claim_id}`), authenticated
and authorized per request. It runs a core instance with no role (it never
opens a store of hooks) and `Ankusa.Gateway` beside it, configured by one YAML
file. The full image, `jamescarr/ankusa`, runs the same gateway when its
`node.roles` lists `claim_check`.

Design: [ADR 0005](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0005-standalone-gateway.md).
Library: [`ankusa_gateway`](https://github.com/jamescarr/ankusa/tree/main/packages/ankusa_gateway).
API contract: [`claim_check.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_gateway/priv/openapi/claim_check.v1.yaml).

## Run

```sh
docker run -d -p 4001:4001 \
  -e ANKUSA_CLAIM_CHECK_IP=0.0.0.0 \
  -e ANKUSA_CLAIM_CHECK_TOKEN="$(openssl rand -hex 32)" \
  -v ./gateway.yml:/etc/ankusa/gateway.yml:ro \
  jamescarr/ankusa-gateway
```

The gateway listens on `127.0.0.1` inside the container unless
`ANKUSA_CLAIM_CHECK_IP` (or `gateway.ip`) says otherwise, and off loopback it
refuses to boot without an authenticator. Workers send the token as
`Authorization: Bearer <token>`.

## Config

`/etc/ankusa/gateway.yml` (`ANKUSA_CONFIG`), the full image's format
(`config_version: 2`) restricted to the sections a gateway reads:
`config_version node log listener metrics tracing secrets egress storage
claim_check gateway`. A section only the full image reads (`sources`,
`dispatch`, `admin`, …) exits 78 naming it. `node.roles`, when present, must be
`[claim_check]`. A version-1 file (its `claim_check.port`, `ip`, `tls`,
`auth`, `max_concurrent_reads`, `integrity_check`) is migrated on load.

```yaml
config_version: 2
node:
  data_dir: /var/lib/ankusa
metrics:
  enabled: true            # /metrics, /health, /ready on 4004
claim_check:
  store:                   # where the dispatch nodes write claim packs
    type: s3
    s3: {bucket: "${CLAIMS_BUCKET}", region: us-east-1}
gateway:
  port: 4001
  ip: 0.0.0.0
  auth:
    authenticators:
      - type: jwt
        issuer: https://login.example.com/
        audience: ankusa
        jwks_url: https://login.example.com/.well-known/jwks.json
```

Every key: [`docs/configuration.md`](https://github.com/jamescarr/ankusa/blob/main/docs/configuration.md#the-gateway-config_version-2).

| Variable | Field |
| --- | --- |
| `ANKUSA_CONFIG` | the config file |
| `ANKUSA_ROLES` | `node.roles` (`claim_check` only) |
| `ANKUSA_DATA_DIR`, `ANKUSA_REGION` | `node.data_dir`, `node.region` |
| `ANKUSA_LOG_LEVEL`, `ANKUSA_LOG_FORMAT` | `log.level`, `log.format` |
| `ANKUSA_CLAIM_CHECK_PORT`, `ANKUSA_CLAIM_CHECK_IP` | `gateway.port`, `gateway.ip` |
| `ANKUSA_CLAIM_CHECK_TOKEN` | `gateway.auth.authenticators`: one token holding `claims:read`, `tenant:*` |
| `ANKUSA_STORAGE_TYPE`, `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_GCS_BUCKET` | `storage.*` |
| `ANKUSA_METRICS_ENABLED`, `ANKUSA_METRICS_PORT`, `ANKUSA_METRICS_IP` | `metrics.*` |

## Commands

`docker run jamescarr/ankusa-gateway <command>`: `start` (default),
`check-config` (exit 0, or 78 with the error), `print-config` (the effective
config as redacted JSON, the gateway's under `"gateway"`), `version`,
`healthcheck` (the image's `HEALTHCHECK`: `GET /ready` on 4001, else 4004),
`remote` and `rpc EXPR` (loopback distribution, from `docker exec`).

## Ports

| Port | |
| --- | --- |
| 4001 | the gateway: `GET /health`, `GET /ready`, `GET\|HEAD /v1/claims/…` |
| 4004 | `metrics.enabled`: `/metrics` (`ankusa_gateway_*` included), `/health`, `/ready` |
