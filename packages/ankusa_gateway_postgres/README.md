# ankusa_gateway_postgres

The Postgres adapter for the Ankusa gateway's `table` authorizer: permissions
that already live in a customer's own tables become a gateway policy without a
denormalised copy.

```elixir
def deps do
  [
    {:ankusa, "~> 0.8"},
    {:ankusa_gateway, "~> 0.1"},
    {:ankusa_gateway_postgres, "~> 0.1"}
  ]
end
```

`Ankusa.Gateway.Postgres.Source` implements
`Ankusa.Gateway.Authz.Table.Source`. It runs the operator's own query and turns
every row into a permission row of `principal`, `action`, `space`, `prefix`
and `effect` — the join is the customer's, so a `users`/`groups`/`folders`
schema needs no export job:

```yaml
gateway:
  authz:
    authorizers:
      team:
        type: table
        mode: snapshot          # every refresh_ms; or lookup for one query per miss
        source:
          type: postgres
          url: postgres://permissions:${DB_PASSWORD}@db.internal:5432/permissions?sslmode=require
          query: SELECT * FROM gateway_permissions
          version_query: SELECT max(updated_at) FROM role_bindings
    policies:
      documents:
        mode: first
        steps: [team]
  spaces:
    docs:
      store: {type: s3, s3: {bucket: docs, region: eu-west-1}}
      policy: documents
```

`gateway_permissions` is a view the operator writes, one row per
`(principal, action, space, prefix)`; see `Ankusa.Gateway.Authz.Table` for the
row rules. `mode: lookup` instead sends one query per cache miss, with the
caller's identities, the action, the space and the key as `$1..$4` (never as
SQL text):

```yaml
          mode: lookup
          source:
            type: postgres
            hostname: db.internal
            database: permissions
            username: gateway
            password: ${DB_PASSWORD}
            lookup_query: |
              SELECT principal, action, space, prefix, effect
              FROM gateway_permissions
              WHERE principal = ANY($1) AND action IN ($2, '*')
                AND space IN ($3, '*')
                AND (prefix = '' OR $4 = prefix
                     OR (right(prefix, 1) = '/' AND starts_with($4, prefix)))
```

Match prefixes with `starts_with`, not `LIKE prefix || '%'`: a `_` or `%` in
a stored prefix is a wildcard to `LIKE`. The gateway also re-checks every row
a lookup returns (its principal, action, space and prefix must match the
request as a snapshot row would) and ignores the others, so a query that
returns too much never grants more.

Every statement runs inside a read-only transaction, and every connection is
TLS by default (off only for a loopback host). `pool_size`, `timeout_ms` and
`version_query` are described in the module's documentation.

## Development

```sh
docker compose up -d --wait   # Postgres on 127.0.0.1:5499
mix test
docker compose down -v
```
