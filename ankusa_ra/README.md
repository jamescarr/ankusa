# ankusa_ra

Shared, multi-node `Ankusa.WAL` adapter backed by a [Ra](https://github.com/rabbitmq/ra)
(Raft) log, for the Ankusa webhook ingestion framework.

`Ankusa.WAL.Postgres` is the other shared adapter, and it works — but it has
structural limits for a fleet: every append takes a fleet-wide advisory lock and
holds it until COMMIT, surviving the loss of the database box depends on
Postgres replication nobody configures for you, and there is no lease on either
cursor, so `:dispatch` and `:storage` must run as single replicas. This adapter
replaces all three:

| | Postgres | Ra |
|---|---|---|
| Append serialization | advisory lock held to COMMIT, fleet-wide | the Raft leader, one writer |
| Survives loss of one node | only with Postgres replication | yes, majority replicated |
| Cursor ownership | single replica, unenforced | `:dispatch`/`:storage` leases with fencing tokens |
| Log reclamation | `DELETE` + `pg_total_relation_size` churn | Raft snapshot + segment spool |

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.1"},
    {:ankusa_ra, "~> 0.1"}
  ]
end
```

## Shape 1 — a dedicated WAL cluster

Three `:wal`-only nodes, each with its own volume, and clients reaching them
over Erlang distribution. The client nodes host no Raft member; they need the
cookie and the member names, nothing else.

```yaml
# wal nodes: ANKUSA_ROLES=wal
roles: [wal]
data_dir: /var/lib/ankusa

# every other node
roles: [edge]
wal:
  type: ra
  members:
    - ankusa_wal_default@ankusa-wal-0
    - ankusa_wal_default@ankusa-wal-1
    - ankusa_wal_default@ankusa-wal-2
```

`members` are `{cluster_name, node}`. The cluster name is fixed to
`:"ankusa_wal_<instance>"`, so in configuration you only name the nodes; a
single `wal` StatefulSet therefore backs exactly one Ankusa instance.

## Shape 2 — one node

The laptop shape: every role, including `wal`, on one machine.

```elixir
config :ankusa,
  roles: [:edge, :dispatch, :storage, :wal],
  wal: {Ankusa.WAL.Ra, members: [{:"ankusa_wal_default", node()}]}
```

A one-member cluster is a real Raft cluster: it elects itself and commits
immediately. It is not fault tolerant — it is the same promise as
`Ankusa.WAL.DiskLog`, with the same API as the fleet shape, which is what makes
local development honest.

## Operations

`Ankusa.WAL.Ra` handles the ordinary case — a member that is missing starts up
and catches up on its own. Two things an operator does by hand:

```sh
# Membership: add or remove a member, or move the leadership aside
mix ankusa.wal.members list     --instance default
mix ankusa.wal.members add      --instance default --node ankusa@wal-3
mix ankusa.wal.members remove   --instance default --node ankusa@wal-3
mix ankusa.wal.members transfer --instance default --node ankusa@wal-1

# Offline cutover from a Postgres WAL
mix ankusa.wal.migrate --from-postgres $DATABASE_URL --instance default \
  --members ankusa_wal_default@wal-0,ankusa_wal_default@wal-1,ankusa_wal_default@wal-2
```

## Testing

No services, no Docker: the suite starts real `:peer` nodes.

```sh
mix test
MAX_RUNS=5000 mix test test/wal_ra_property_test.exs
```

See `Ankusa.WAL.Ra` for configuration and semantics, and
[`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the framework.
