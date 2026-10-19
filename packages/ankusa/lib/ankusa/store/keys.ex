defmodule Ankusa.Store.Keys do
  @moduledoc false

  # Key encoders and decoders for `Ankusa.Store`. Every key is a binary (the
  # RocksDB NIFs reject iolists) and every integer is big-endian, so byte order
  # is sort order. Sentinel sink indexes (0xFFFE/0xFFFF) exist only for rows
  # imported from a 0.3 data dir.
  #
  # Scanned key families each own a low and a high *sentinel* key, written once
  # when the store opens (`sentinels/0`). The RocksDB binding reports a
  # corruption met during iteration as a clean end of the scan, so
  # `Ankusa.Store.fold/6` only believes a scan that reaches a sentinel (or a key
  # beyond the requested range). A sentinel is never handed to a caller.

  # ── default column family ────────────────────────────────────────────────

  def meta("next_seq"), do: "m:next_seq"
  # The next tenant ordinal to hand out (`<<next::32>>`), see `Ankusa.Queue.Tenants`.
  def meta("next_ord"), do: "m:next_ord"
  # When the store format 2 -> 3 migration started (8 bytes, ms): rows due after
  # it are retries, the rest first attempts.
  def meta("tenant_migration_at"), do: "m:tenant_migration_at"
  def meta("migration"), do: "m:migration"
  def meta("ready_probe"), do: "m:ready_probe"
  def meta("store_id"), do: "m:store_id"
  # Lifecycle event identity across a restore (`Ankusa.Audit`): `incarnation` is
  # the history of fresh ids a restore from a backup mints (JSON, newest first),
  # `incarnation_pending` the id a restore wrote and the audit trail has not yet
  # announced. `intake_offset` is the last intake-log offset the store holds
  # (written by the intake log, carried by every backup manifest).
  def meta("incarnation"), do: "m:incarnation"
  def meta("incarnation_pending"), do: "m:incarnation_pending"
  def meta("intake_offset"), do: "m:intake_offset"
  # The audit trail's size (`<<rows::64, bytes::64, newest_counted_seq::64>>`),
  # written in every batch that adds or removes a row, so `audit.max_rows` and
  # `audit.max_bytes` are checked in O(1) (`Ankusa.Audit`). A value that does not
  # match the newest row of the trail is recounted at boot.
  def meta("audit_usage"), do: "m:audit_usage"
  def meta("imported:" <> artifact), do: "m:imported:" <> artifact

  # The store's layout version (an integer in decimal), and the resumable marker
  # of the format migration that is moving it to `to` (`Ankusa.Store.Migrate`).
  def format_version, do: "m:format_version"
  def migrating(to) when is_integer(to) and to > 0, do: "m:migrating:" <> Integer.to_string(to)

  def source(tenant, name) when is_binary(tenant) and is_binary(name),
    do: <<"s:", tenant::binary, 0, name::binary>>

  def rate_limit(tenant) when is_binary(tenant), do: <<"r:", tenant::binary>>

  # A source's revision: a per-subject counter (decimal) that outlives the
  # source's deletion, so a re-created source continues the count.
  def source_revision(tenant, name) when is_binary(tenant) and is_binary(name),
    do: <<"v:", tenant::binary, 0, name::binary>>

  # The last sequence number the audit trail handed out, written in every audit
  # batch, and the HMAC key audit hashes are made with (`Ankusa.Audit`).
  def audit_seq, do: "m:audit_seq"
  def audit_key, do: "m:audit_key"

  # The last audit `seq` the lifecycle publisher has delivered to every
  # lifecycle sink (`Ankusa.Lifecycle.Publisher`): 8 bytes, big-endian.
  def lifecycle_cursor, do: "m:lifecycle_cursor"

  # ── hooks ─────────────────────────────────────────────────────────────────

  def hook(seq), do: <<seq::64>>

  # ── deliveries ────────────────────────────────────────────────────────────

  def delivery(seq, sink), do: <<seq::64, sink::16>>

  # ── index ─────────────────────────────────────────────────────────────────

  # The v3 index (ADR 0001): due, inflight and the by-tenant dead index lead
  # with the tenant ordinal, so one tenant's rows are one contiguous range.
  # `dead` keeps its global time-ordered key (`<<?x, at, seq, sink>>`); its value
  # carries the ordinal. No code outside `Ankusa.Queue.Deliveries` builds a
  # `?D`, `?F`, `?X` or `?A` key.
  def due(ord, at, seq, sink), do: <<?D, ord::32, at::64, seq::64, sink::16>>
  def inflight(ord, seq, sink), do: <<?F, ord::32, seq::64, sink::16>>
  def dead(at, seq, sink), do: <<?x, at::64, seq::64, sink::16>>
  def dead_by_tenant(ord, at, seq, sink), do: <<?X, ord::32, at::64, seq::64, sink::16>>
  # One marker per (tenant, second) that has a due row in it.
  def active(bucket, ord), do: <<?A, bucket::64, ord::32>>
  def archive_pending(seq), do: <<?a, seq::64>>

  # The format 2 layout, which only the format 2 -> 3 migration and the 0.3
  # import (`Ankusa.Store.Migrate`, which writes it for the migration to move)
  # still use.
  def due_v2(at, seq, sink), do: <<?d, at::64, seq::64, sink::16>>
  def inflight_v2(seq, sink), do: <<?f, seq::64, sink::16>>
  def cleared(seq, kind, sink), do: <<?c, seq::64, kind::8, sink::16>>
  def claim(seq), do: <<?k, seq::64>>

  # Claim leases (DUR-2.c): one row per (claim pack, hook that references it),
  # `<<?l, tenant, 0, pack_id::binary-26, seq::64>>`, in the index CF so the
  # check-in and the reclaim commit with the hook's own batch. The value is
  # `<<0::64>>` while the hook is live, and `<<expires_at_ms::64>>` once the hook
  # is reclaimed: the pack then stays leased until then (`claim_check.redeem_grace_ms`),
  # so a consumer that gets a queue message just after delivery can still redeem
  # it. `claim_lease/2` is the prefix of every lease of one pack. Tenant ids are
  # `[A-Za-z0-9_-]` and a pack id is a 26-character ULID, so the key parses
  # unambiguously.
  def claim_lease(tenant, pack_id) when is_binary(tenant) and byte_size(pack_id) == 26,
    do: <<?l, tenant::binary, 0, pack_id::binary>>

  def claim_lease(tenant, pack_id, seq), do: <<claim_lease(tenant, pack_id)::binary, seq::64>>

  # ── tenants and counters ──────────────────────────────────────────────────

  # `tenants` column family: `ord -> tenant id` and `tenant id -> ord`. Ordinal 0
  # is the node aggregate; real ordinals start at 1 and are never reused.
  def tenant(ord), do: <<?t, ord::32>>
  def tenant_ord(tenant_id) when is_binary(tenant_id), do: <<?n, tenant_id::binary>>

  # `counters` column family: one merge-operand row per tenant and field, plus
  # the node aggregate under ordinal 0 (`Ankusa.Queue.Deliveries.Counters`).
  def counter(ord, field) when field in 1..9, do: <<ord::32, field::8>>

  # Ingest dedupe: `?u` maps a provider event key to
  # `<<expires_at::64, original_id::binary>>`, and `?e` is its expiry-sweep
  # index. Both live in the index CF so the writer's batch is atomic. Keys
  # are scoped by tenant as well as source: `TenantPath` routing serves one
  # source to many tenants, and two tenants must never collapse each other's
  # events.
  def dedupe(tenant_id, source_id, key),
    do: <<?u, tenant_id::binary, 0, byte_size(source_id)::16, source_id::binary, key::binary>>

  def dedupe_expiry(at, tenant_id, source_id, key),
    do:
      <<?e, at::64, tenant_id::binary, 0, byte_size(source_id)::16, source_id::binary,
        key::binary>>

  # ── archive ───────────────────────────────────────────────────────────────

  def segment(first_seq), do: <<?S, first_seq::64>>
  def legacy_location(event_id) when is_binary(event_id), do: <<?L, event_id::binary>>

  # ── quarantine ────────────────────────────────────────────────────────────

  def quarantine_summary(received_at, id) when is_binary(id),
    do: <<?s, received_at::64, id::binary>>

  def quarantine_body(received_at, id) when is_binary(id),
    do: <<?b, received_at::64, id::binary>>

  # Replay jobs (`Ankusa.Dispatch.Replayer`); `id` is a UUIDv7 string.
  def replay_job(id) when is_binary(id), do: "j:" <> id

  # ── audit ─────────────────────────────────────────────────────────────────

  # The trail, in commit order: `seq` comes first because one process
  # (`Ankusa.Audit`) allocates it and commits the batch, so key order is
  # commit order whatever the wall clock does. A mutation whose store is not
  # this one is recorded as a `?p` row first (`audit_pending/1`) and moves into
  # the trail when it finishes.
  def audit(seq, at) when is_integer(seq) and is_integer(at), do: <<?a, seq::64, at::64>>
  def audit_pending(seq) when is_integer(seq), do: <<?p, seq::64>>

  # The redacted entity a lifecycle event carries, beside the audit row of the
  # same `seq` and in the same batch. It is not in the row: an audit row never
  # holds a spec, and the entity (a source's view, a route) is one even redacted.
  def lifecycle_entity(seq) when is_integer(seq), do: <<?o, seq::64>>

  # ── decoders ──────────────────────────────────────────────────────────────

  def decode_due(<<?D, ord::32, at::64, seq::64, sink::16>>), do: {ord, at, seq, sink}
  def decode_inflight(<<?F, ord::32, seq::64, sink::16>>), do: {ord, seq, sink}
  def decode_dead(<<?x, at::64, seq::64, sink::16>>), do: {at, seq, sink}
  def decode_dead_by_tenant(<<?X, ord::32, at::64, seq::64, sink::16>>), do: {ord, at, seq, sink}
  def decode_active(<<?A, bucket::64, ord::32>>), do: {bucket, ord}
  def decode_due_v2(<<?d, at::64, seq::64, sink::16>>), do: {at, seq, sink}
  def decode_inflight_v2(<<?f, seq::64, sink::16>>), do: {seq, sink}
  def decode_cleared(<<?c, seq::64, kind::8, sink::16>>), do: {seq, kind, sink}
  def decode_delivery(<<seq::64, sink::16>>), do: {seq, sink}

  def decode_claim_lease(<<?l, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [tenant, <<pack_id::binary-size(26), seq::64>>] -> {tenant, pack_id, seq}
      _ -> :error
    end
  end

  def decode_audit(<<?a, seq::64, at::64>>), do: {seq, at}
  def decode_lifecycle_entity(<<?o, seq::64>>), do: seq
  def decode_audit_pending(<<?p, seq::64>>), do: seq
  def decode_source_revision(<<"v:", rest::binary>>), do: decode_source(<<"s:", rest::binary>>)

  def decode_dedupe_expiry(<<?e, at::64, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [tenant_id, <<n::16, source_id::binary-size(n), key::binary>>] ->
        {at, tenant_id, source_id, key}

      _ ->
        :error
    end
  end

  def decode_source(<<"s:", rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [tenant, name] -> {tenant, name}
      _ -> nil
    end
  end

  def decode_rate_limit(<<"r:", tenant::binary>>), do: tenant

  # ── scan families and sentinels ───────────────────────────────────────────

  # `lo` sorts below every real key of the family and `hi` above every one.
  # Real keys can only collide with a sentinel if every field is all-ones/zero,
  # which none of them can be (seqs start at 1; times are real milliseconds).
  @ff8 <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
  @ff10 <<@ff8::binary, 0xFF, 0xFF>>
  @ff11 <<@ff10::binary, 0xFF>>
  @ff18 <<@ff10::binary, @ff8::binary>>
  @ff16 <<@ff8::binary, @ff8::binary>>
  @ff4 <<0xFF, 0xFF, 0xFF, 0xFF>>

  @families %{
    hooks: %{cf: :hooks, lo: <<0::64>>, hi: @ff8},
    deliveries: %{cf: :deliveries, lo: <<0::80>>, hi: @ff10},
    due: %{cf: :index, lo: <<?D>>, hi: <<?D, @ff4::binary, @ff18::binary>>},
    inflight: %{cf: :index, lo: <<?F>>, hi: <<?F, @ff4::binary, @ff10::binary>>},
    dead: %{cf: :index, lo: <<?x>>, hi: <<?x, @ff18::binary>>},
    dead_by_tenant: %{cf: :index, lo: <<?X>>, hi: <<?X, @ff4::binary, @ff18::binary>>},
    active: %{cf: :index, lo: <<?A>>, hi: <<?A, @ff8::binary, @ff4::binary>>},
    tenants: %{cf: :tenants, lo: <<?t>>, hi: <<?t, @ff4::binary>>},
    tenant_ords: %{cf: :tenants, lo: <<?n>>, hi: <<?n, 0xFF>>},
    counters: %{cf: :counters, lo: <<0::40>>, hi: <<@ff4::binary, 0xFF>>},
    archive_pending: %{cf: :index, lo: <<?a>>, hi: <<?a, @ff8::binary>>},
    cleared: %{cf: :index, lo: <<?c>>, hi: <<?c, @ff11::binary>>},
    claims: %{cf: :index, lo: <<?k>>, hi: <<?k, @ff8::binary>>},
    claim_leases: %{cf: :index, lo: <<?l>>, hi: <<?l, 0xFF>>},
    dedupe_expiry: %{cf: :index, lo: <<?e>>, hi: <<?e, @ff8::binary, 0xFF>>},
    segments: %{cf: :archive, lo: <<?S>>, hi: <<?S, @ff8::binary>>},
    legacy_locations: %{cf: :archive, lo: <<?L>>, hi: <<?L, 0xFF>>},
    quarantine: %{cf: :quarantine, lo: <<?s>>, hi: <<?s, @ff8::binary>>},
    sources: %{cf: :default, lo: "s:", hi: <<"s:", 0xFF>>},
    rate_limits: %{cf: :default, lo: "r:", hi: <<"r:", 0xFF>>},
    replays: %{cf: :default, lo: "j:", hi: <<"j:", 0xFF>>},
    source_revisions: %{cf: :default, lo: "v:", hi: <<"v:", 0xFF>>},
    audit: %{cf: :audit, lo: <<?a>>, hi: <<?a, @ff16::binary>>},
    audit_pending: %{cf: :audit, lo: <<?p>>, hi: <<?p, @ff8::binary>>},
    lifecycle_entities: %{cf: :audit, lo: <<?o>>, hi: <<?o, @ff8::binary>>}
  }

  # Families that exist only for the format 2 -> 3 migration to read (`?d`,
  # `?f`): scanned like any other, but their sentinels are written by the
  # migration, not on every open, so the migration's final range delete is the
  # last of them.
  @legacy_families %{
    due_v2: %{cf: :index, lo: <<?d>>, hi: <<?d, @ff18::binary>>},
    inflight_v2: %{cf: :index, lo: <<?f>>, hi: <<?f, @ff10::binary>>}
  }

  @doc "The column family, low and high sentinel of a scanned key family."
  def family(name), do: Map.get(@families, name) || Map.fetch!(@legacy_families, name)

  @doc "Every `{cf, key}` sentinel `Ankusa.Store` writes when it opens."
  def sentinels do
    Enum.flat_map(@families, fn {_name, %{cf: cf, lo: lo, hi: hi}} -> [{cf, lo}, {cf, hi}] end)
  end

  @doc "The `{cf, key}` sentinels of the format 2 families, for the migration that reads them."
  def legacy_sentinels do
    Enum.flat_map(@legacy_families, fn {_name, %{cf: cf, lo: lo, hi: hi}} ->
      [{cf, lo}, {cf, hi}]
    end)
  end

  @doc "`{lower, upper}` for everything that starts with `prefix`."
  def range(prefix), do: {prefix, successor(prefix)}

  # The first key that does not start with `prefix`: increment with carry.
  defp successor(bin), do: do_successor(bin, byte_size(bin) - 1)

  defp do_successor(bin, i) when i >= 0 do
    case :binary.at(bin, i) do
      255 -> do_successor(bin, i - 1)
      b -> <<binary_part(bin, 0, i)::binary, b + 1>>
    end
  end

  defp do_successor(_bin, -1) do
    raise ArgumentError, "store key prefix is all 0xFF bytes"
  end
end
