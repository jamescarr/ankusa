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
  def meta("migration"), do: "m:migration"
  def meta("ready_probe"), do: "m:ready_probe"
  def meta("imported:" <> artifact), do: "m:imported:" <> artifact

  def source(tenant, name) when is_binary(tenant) and is_binary(name),
    do: <<"s:", tenant::binary, 0, name::binary>>

  def rate_limit(tenant) when is_binary(tenant), do: <<"r:", tenant::binary>>

  # ── hooks ─────────────────────────────────────────────────────────────────

  def hook(seq), do: <<seq::64>>

  # ── deliveries ────────────────────────────────────────────────────────────

  def delivery(seq, sink), do: <<seq::64, sink::16>>

  # ── index ─────────────────────────────────────────────────────────────────

  def due(at, seq, sink), do: <<?d, at::64, seq::64, sink::16>>
  def inflight(seq, sink), do: <<?f, seq::64, sink::16>>
  def dead(at, seq, sink), do: <<?x, at::64, seq::64, sink::16>>
  def archive_pending(seq), do: <<?a, seq::64>>
  def cleared(seq, kind, sink), do: <<?c, seq::64, kind::8, sink::16>>
  def claim(seq), do: <<?k, seq::64>>

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

  # ── decoders ──────────────────────────────────────────────────────────────

  def decode_due(<<?d, at::64, seq::64, sink::16>>), do: {at, seq, sink}
  def decode_inflight(<<?f, seq::64, sink::16>>), do: {seq, sink}
  def decode_dead(<<?x, at::64, seq::64, sink::16>>), do: {at, seq, sink}
  def decode_cleared(<<?c, seq::64, kind::8, sink::16>>), do: {seq, kind, sink}
  def decode_delivery(<<seq::64, sink::16>>), do: {seq, sink}

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

  @families %{
    hooks: %{cf: :hooks, lo: <<0::64>>, hi: @ff8},
    deliveries: %{cf: :deliveries, lo: <<0::80>>, hi: @ff10},
    due: %{cf: :index, lo: <<?d>>, hi: <<?d, @ff18::binary>>},
    inflight: %{cf: :index, lo: <<?f>>, hi: <<?f, @ff10::binary>>},
    dead: %{cf: :index, lo: <<?x>>, hi: <<?x, @ff18::binary>>},
    archive_pending: %{cf: :index, lo: <<?a>>, hi: <<?a, @ff8::binary>>},
    cleared: %{cf: :index, lo: <<?c>>, hi: <<?c, @ff11::binary>>},
    dedupe_expiry: %{cf: :index, lo: <<?e>>, hi: <<?e, @ff8::binary, 0xFF>>},
    segments: %{cf: :archive, lo: <<?S>>, hi: <<?S, @ff8::binary>>},
    quarantine: %{cf: :quarantine, lo: <<?s>>, hi: <<?s, @ff8::binary>>},
    sources: %{cf: :default, lo: "s:", hi: <<"s:", 0xFF>>},
    rate_limits: %{cf: :default, lo: "r:", hi: <<"r:", 0xFF>>},
    replays: %{cf: :default, lo: "j:", hi: <<"j:", 0xFF>>}
  }

  @doc "The column family, low and high sentinel of a scanned key family."
  def family(name), do: Map.fetch!(@families, name)

  @doc "Every `{cf, key}` sentinel `Ankusa.Store` writes when it opens."
  def sentinels do
    Enum.flat_map(@families, fn {_name, %{cf: cf, lo: lo, hi: hi}} -> [{cf, lo}, {cf, hi}] end)
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
