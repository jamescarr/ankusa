defmodule Ankusa.Sink.Message do
  @moduledoc """
  The wire format every queue-style sink publishes (`Sink.RabbitMQ`,
  `Sink.Kafka`, `Sink.NATS`, `Sink.Redis`, `Sink.SQS`, `Sink.GooglePubSub`), so
  a consumer parses one format regardless of transport.

  A body of at most `inline_max_bytes` (default 64 KiB) rides inline,
  base64-encoded. Anything larger lives in the claim check and the message
  carries its `Ankusa.ClaimCheck.Ref` as one string instead, plus the
  lowercase hex sha256 the reader checks the redeemed bytes against (see
  `docs/claim-check.md`). Every message carries `sha256`, `dedupe_key`,
  `replay_id`, `idempotency_key` (the tenant-scoped key a consumer dedupes on:
  `Ankusa.Envelope.idempotency_key/1`), `verification` (how the hook was
  verified at ingest: `status` is `"ok"`, `"failed"` or `"skipped"`, and
  `flagged` is `true` when a failed check was let through by the source's
  `on_verify_failure: accept_flag`, so a consumer treats that hook as
  unverified), the forwarded provider `headers` and the W3C trace context
  `traceparent` / `tracestate` (`Ankusa.Trace`: strings, or `null` for a hook
  that carries none; a consumer continues the trace from them):

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "dedupe_key": "evt_1", "replay_id": null, "idempotency_key": "acme:stripe:evt_1",
       "verification": {"status": "ok", "flagged": false},
       "headers": {"x-github-event": "push"},
       "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
       "tracestate": null,
       "sha256": "2cf24dba...", "body_base64": "eyJpZCI6..."}

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 3145728,
       "dedupe_key": "evt_1", "replay_id": null, "idempotency_key": "acme:stripe:evt_1",
       "verification": {"status": "ok", "flagged": false},
       "headers": {},
       "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002",
       "sha256": "3bea8a9a07c1e8dc..."}

  `v` changes only when an existing field changes meaning or disappears.
  Adding a field keeps `v: 1`; consumers must ignore keys they don't know. A hook of
  an ordered sink (`Ankusa.Ordering`) also carries `ordering_key`, the key of its
  lane (`"tenant/source"` for the sink's keyless lane): the only place a sink with
  no headers, `Sink.Redis`, can show a consumer which lane a message is in.

  ## Size budget

  Base64 inflates the inline body by 4/3, so an inline message is about
  `inline_max_bytes * 4/3` plus the metadata: ~1 KiB of envelope fields plus
  the forwarded `headers` (at most `forward_headers_max_bytes`, 16 KiB, in at
  most `max_headers`, 64, per source). The 64 KiB default is about 106 KiB
  encoded, under every transport's limit (Kafka `max.message.bytes`, 1 MiB by
  default; NATS `max_payload`, 1 MiB; SQS, 1 MiB).

  `encode/4` enforces that budget before anything is published, against the
  sink's `max_bytes/1` (`c:Ankusa.Sink.Messaging.max_message_bytes/1`):

    * an inline body whose message is over the limit goes to the claim check
      instead, so a raised `inline_max_bytes` degrades to a claim, not to a
      message the broker refuses. That claim is checked in by the attempt, not
      by dispatch, so it is not persisted: a retry checks in again, and the
      orphaned object is swept by claim retention;
    * a body that is over the limit by itself (`size > max`, with
      `inline_max_bytes` at least `size`) cannot be shrunk by any retry:
      `{:error, {:permanent, {:message_too_large, size, max}}}`, dead-lettered
      at once, with no broker call and no claim written;
    * a claim message over the limit (`max` below the ~1 KiB the envelope
      needs) is permanent too.

  Forwarded headers are capped in `forwarded_headers/2`: a header that would
  take a message past either cap is dropped, and each drop is counted
  (`ankusa_sink_headers_dropped_total{reason="count" | "bytes"}`).
  """

  alias Ankusa.{ClaimCheck, Envelope}
  alias Ankusa.ClaimCheck.Ref

  @default_inline_max_bytes 65_536
  @default_max_message_bytes 1_048_576
  @default_forward_headers_max_bytes 16_384
  @default_max_headers 64

  # Never leaves the node: request headers that only mean something to the
  # edge (auth, framing, hop-by-hop) are stripped before forwarding, plus
  # every `x-ankusa-*` header the sink itself owns.
  @never_forwarded ~w(authorization proxy-authorization cookie x-api-key host content-length content-type connection keep-alive transfer-encoding te trailer upgrade expect)

  @doc "The default `inline_max_bytes` for every queue-style sink: 64 KiB."
  @spec default_inline_max_bytes() :: pos_integer()
  def default_inline_max_bytes, do: @default_inline_max_bytes

  @doc "A sink's configured `:inline_max_bytes`, or the default."
  @spec inline_max_bytes(keyword()) :: pos_integer()
  def inline_max_bytes(opts), do: Keyword.get(opts, :inline_max_bytes, @default_inline_max_bytes)

  @doc "The default largest message a queue-style sink publishes: 1 MiB."
  @spec default_max_message_bytes() :: pos_integer()
  def default_max_message_bytes, do: @default_max_message_bytes

  @doc """
  A sink's configured `:max_message_bytes`, or the default (1 MiB). A sink
  whose transport has another limit, or another option name, overrides
  `c:Ankusa.Sink.Messaging.max_message_bytes/1` instead of calling this.
  """
  @spec max_bytes(keyword()) :: pos_integer()
  def max_bytes(opts), do: Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

  @doc "The default `forward_headers_max_bytes` of a source: 16 KiB."
  @spec default_forward_headers_max_bytes() :: pos_integer()
  def default_forward_headers_max_bytes, do: @default_forward_headers_max_bytes

  @doc "The default `max_headers` of a source: 64."
  @spec default_max_headers() :: pos_integer()
  def default_max_headers, do: @default_max_headers

  @doc """
  Encode `env` for a sink whose threshold is `inline_max_bytes` and whose
  largest message is `max_bytes` (default 1 MiB; see "Size budget").

  A body over the threshold uses the ref dispatch already checked in
  (`ctx.claim`). A sink called outside dispatch has none, so the body is
  checked in here, as a one-claim pack.
  """
  @spec encode(Envelope.t(), Ankusa.Sink.ctx(), pos_integer(), pos_integer()) ::
          {:ok, binary()}
          | {:error, {:claim_check, ClaimCheck.reason()}}
          | {:error, {:permanent, {:message_too_large, non_neg_integer(), pos_integer()}}}
  def encode(%Envelope{} = env, ctx, inline_max_bytes, max_bytes \\ @default_max_message_bytes)
      when is_integer(inline_max_bytes) and inline_max_bytes > 0 and is_integer(max_bytes) and
             max_bytes > 0 do
    base =
      %{
        v: 1,
        id: env.id,
        source_id: env.source_id,
        tenant_id: env.tenant_id,
        received_at: env.received_at,
        content_type: env.content_type,
        size: env.size,
        dedupe_key: env.dedupe_key,
        replay_id: Map.get(ctx, :replay_id),
        idempotency_key: Envelope.idempotency_key(env),
        verification: verification(env.verification),
        headers: forwarded_headers(env, ctx),
        traceparent: trace_value(env.trace, :traceparent),
        tracestate: trace_value(env.trace, :tracestate)
      }
      |> put_ordering_key(ctx)

    cond do
      env.size > inline_max_bytes ->
        claimed(base, ctx, env, max_bytes)

      env.size > max_bytes ->
        {:error, {:permanent, {:message_too_large, env.size, max_bytes}}}

      true ->
        json =
          JSON.encode!(
            Map.merge(base, %{
              body_base64: Base.encode64(env.body),
              sha256: Base.encode16(:crypto.hash(:sha256, env.body), case: :lower)
            })
          )

        if byte_size(json) <= max_bytes, do: {:ok, json}, else: claimed(base, ctx, env, max_bytes)
    end
  end

  # What the `claim` and `sha256` fields add to a message even with an empty
  # ref: a floor, so a message that cannot fit however small its ref is refused
  # before anything is checked in.
  @claim_fields_floor byte_size(~s(,"claim":"","sha256":"")) + 64

  defp claimed(base, ctx, env, max_bytes) do
    with :ok <- fits(byte_size(JSON.encode!(base)) + @claim_fields_floor, max_bytes) do
      case claim(ctx, env) do
        {:ok, %{ref: ref, sha256: sha256}} ->
          json = JSON.encode!(Map.merge(base, %{claim: Ref.to_string(ref), sha256: sha256}))

          case fits(byte_size(json), max_bytes) do
            :ok -> {:ok, json}
            {:error, _permanent} = error -> error
          end

        {:error, reason} ->
          {:error, {:claim_check, reason}}
      end
    end
  end

  @doc """
  The metadata a queue sink carries outside the body, as `{name, value}` pairs in
  this order: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
  `ankusa_message_version` ("1"), `ankusa_idempotency_key`, `content_type`
  ("application/json"), `ankusa_dedupe_key`, `ankusa_replay_id`,
  `ankusa_ordering_key` (the lane's key, of a hook of an ordered sink). A pair
  whose value is `nil` or `""` is left out (no tenant, no dedupe key, not a
  replay, not ordered). A hook that carries a trace context (`Ankusa.Trace`)
  adds `traceparent` and, when it has one, `tracestate`, last.
  """
  @spec attributes(Envelope.t(), Ankusa.Sink.ctx()) :: [{String.t(), String.t()}]
  def attributes(%Envelope{} = env, ctx) do
    [
      {"ankusa_id", env.id},
      {"ankusa_source_id", env.source_id},
      {"ankusa_tenant_id", env.tenant_id},
      {"ankusa_message_version", "1"},
      {"ankusa_idempotency_key", Envelope.idempotency_key(env)},
      {"content_type", "application/json"},
      {"ankusa_dedupe_key", env.dedupe_key},
      {"ankusa_replay_id", ctx[:replay_id]},
      {"ankusa_ordering_key", ctx[:ordering_key]}
    ]
    |> Enum.filter(fn {_name, value} -> is_binary(value) and value != "" end)
    |> Kernel.++(trace_pairs(env.trace))
  end

  @doc """
  The metadata a sink with transport headers carries beside the body
  (`Ankusa.Sink.Kafka`, `Ankusa.Sink.NATS`), as `{name, value}` pairs in this
  order: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
  `ankusa_message_version` ("1"), `ankusa_idempotency_key`, `content_type`
  ("application/json"), then `ankusa_dedupe_key` and `ankusa_replay_id` when
  the hook carries them, then `traceparent` and `tracestate` when it carries a
  trace context (`Ankusa.Trace`). Unlike `attributes/2`, `ankusa_tenant_id` is
  always present — `""` without a tenant — because such a sink's description
  promises the header set (`ankusa_headers: true`) and the AsyncAPI header
  schema requires it.
  """
  @spec headers(Envelope.t(), Ankusa.Sink.ctx()) :: [{String.t(), String.t()}]
  def headers(%Envelope{} = env, ctx) do
    [
      {"ankusa_id", env.id},
      {"ankusa_source_id", env.source_id},
      {"ankusa_tenant_id", env.tenant_id || ""},
      {"ankusa_message_version", "1"},
      {"ankusa_idempotency_key", Envelope.idempotency_key(env)},
      {"content_type", "application/json"}
    ] ++
      for(
        {name, value} <- [
          {"ankusa_dedupe_key", env.dedupe_key},
          {"ankusa_replay_id", ctx[:replay_id]},
          {"ankusa_ordering_key", ctx[:ordering_key]}
        ],
        is_binary(value),
        do: {name, value}
      ) ++ trace_pairs(env.trace)
  end

  defp put_ordering_key(base, %{ordering_key: key}) when is_binary(key),
    do: Map.put(base, :ordering_key, key)

  defp put_ordering_key(base, _ctx), do: base

  # The W3C trace context of the hook, as the pairs a broker carries natively
  # (Kafka and NATS headers, SQS and Pub/Sub attributes): `traceparent`, and
  # `tracestate` when there is one. None for a hook with no trace.
  defp trace_pairs(%{traceparent: traceparent} = trace) do
    [{"traceparent", traceparent}] ++
      case Map.get(trace, :tracestate) do
        state when is_binary(state) and state != "" -> [{"tracestate", state}]
        _ -> []
      end
  end

  defp trace_pairs(_no_trace), do: []

  defp trace_value(%{} = trace, key), do: Map.get(trace, key)
  defp trace_value(_no_trace, _key), do: nil

  @doc """
  The id a broker dedupes one delivery on: the hook's `id`, or
  `id:replay:<replay_id>` for a replay, so a retry after a lost ack collapses
  while a deliberate replay of the same hook is stored again (SQS's
  `MessageDeduplicationId`, NATS's `Nats-Msg-Id`).
  """
  @spec delivery_id(Envelope.t(), Ankusa.Sink.ctx()) :: String.t()
  def delivery_id(%Envelope{id: id}, ctx) do
    case ctx[:replay_id] do
      replay_id when is_binary(replay_id) -> id <> ":replay:" <> replay_id
      _ -> id
    end
  end

  @doc """
  Refuse a message the transport would never take, before sending it:
  `size` bytes (the sink's own accounting — value plus key, body plus
  attributes) against `max`. Over it is `{:error, {:permanent,
  {:message_too_large, size, max}}}`, dead-lettered at once.
  """
  @spec fits(non_neg_integer(), pos_integer()) ::
          :ok | {:error, {:permanent, {:message_too_large, non_neg_integer(), pos_integer()}}}
  def fits(size, max) when size > max, do: {:error, {:permanent, {:message_too_large, size, max}}}
  def fits(_size, _max), do: :ok

  @doc """
  The provider request headers a sink may see, per the source's
  `forward_headers` option — and capped, so a provider with large headers
  cannot push a small body over a transport limit.

  The second argument is either the forwarding mode on its own or a sink
  `t:Ankusa.Sink.ctx/0`, whose `:forward_headers`, `:max_headers` (default 64)
  and `:forward_headers_max_bytes` (default 16 KiB) apply:

  * `:default` forwards every header except the never-forwarded set.
  * A list of names forwards only those, still minus the never-forwarded set.
  * `[]` forwards nothing.

  Names are lowercased; repeated names are joined with `", "` in arrival
  order. Never forwarded: `#{Enum.join(@never_forwarded, "` `")}`, plus any
  `x-ankusa-*` header. A header the stored-header policy dropped
  (`Ankusa.Edge.StoredHeaders`) is not in the hook at all, so no
  `forward_headers` setting brings it back; a hashed one is forwarded as its
  `sha256:<hex>` digest.

  Caps apply to the headers that would be forwarded, in order of first
  arrival. A name is kept while fewer than `max_headers` are kept and its
  name plus joined value fits in what is left of `forward_headers_max_bytes`;
  otherwise it is dropped whole (a header is never truncated), and later,
  smaller ones are still considered. With a ctx, each reason that dropped
  anything emits `[:ankusa, :sink, :headers_dropped]` once, with `count` the
  number dropped and `reason` `:count` or `:bytes`.
  """
  @spec forwarded_headers(Envelope.t(), :default | [String.t()] | Ankusa.Sink.ctx()) :: %{
          String.t() => String.t()
        }
  def forwarded_headers(env, mode_or_ctx \\ :default)

  def forwarded_headers(%Envelope{} = env, ctx) when is_map(ctx) do
    {kept, dropped} =
      env
      |> forward(allowed(Map.get(ctx, :forward_headers) || :default))
      |> cap(
        Map.get(ctx, :max_headers) || @default_max_headers,
        Map.get(ctx, :forward_headers_max_bytes) || @default_forward_headers_max_bytes
      )

    for {reason, count} <- dropped do
      Ankusa.Telemetry.emit([:sink, :headers_dropped], %{count: count}, %{
        instance: Map.get(ctx, :instance),
        source_id: env.source_id,
        tenant_id: env.tenant_id,
        reason: reason
      })
    end

    kept
  end

  def forwarded_headers(%Envelope{} = env, mode) do
    {kept, _dropped} =
      env
      |> forward(allowed(mode))
      |> cap(@default_max_headers, @default_forward_headers_max_bytes)

    kept
  end

  defp allowed(:default), do: nil
  defp allowed(names) when is_list(names), do: MapSet.new(names, &String.downcase/1)

  # `[{name, value}]` in order of first arrival, repeated names joined.
  defp forward(%Envelope{headers: headers}, allowed) do
    {joined, order} =
      Enum.reduce(headers, {%{}, []}, fn {name, value}, {acc, order} = state ->
        name = String.downcase(name)

        cond do
          String.starts_with?(name, "x-ankusa-") -> state
          name in @never_forwarded -> state
          allowed != nil and not MapSet.member?(allowed, name) -> state
          true -> join(acc, order, name, value)
        end
      end)

    for name <- Enum.reverse(order), do: {name, Map.fetch!(joined, name)}
  end

  defp join(acc, order, name, value) do
    case acc do
      %{^name => previous} -> {%{acc | name => previous <> ", " <> value}, order}
      _ -> {Map.put(acc, name, value), [name | order]}
    end
  end

  # `{kept_map, %{reason => dropped_count}}`.
  defp cap(pairs, max_headers, max_bytes) do
    {kept, _count, _bytes, dropped} =
      Enum.reduce(pairs, {%{}, 0, 0, %{}}, fn {name, value}, {kept, count, bytes, dropped} ->
        size = byte_size(name) + byte_size(value)

        cond do
          count >= max_headers ->
            {kept, count, bytes, Map.update(dropped, :count, 1, &(&1 + 1))}

          bytes + size > max_bytes ->
            {kept, count, bytes, Map.update(dropped, :bytes, 1, &(&1 + 1))}

          true ->
            {Map.put(kept, name, value), count + 1, bytes + size, dropped}
        end
      end)

    {kept, dropped}
  end

  @doc """
  Check `env`'s body in on its own, as a one-claim pack under a fresh pack id
  (`Ref.new_pack_id/0`: the current time and 64 random bits, the entropy every
  batch pack has). Nothing about the id is derivable from the hook, and its
  date partition is the check-in day, so retention counts from when the object
  was written.

  Dispatch persists the ref with the attempt's outcome (`fresh_claim`, see
  `Ankusa.Dispatch.Pipeline`), so a retry reuses it. A crash between the
  check-in and that write orphans one object, which retention sweeps.
  """
  @spec check_in(atom(), Envelope.t()) ::
          {:ok, ClaimCheck.claim()} | {:error, ClaimCheck.reason()}
  def check_in(instance, %Envelope{} = env) do
    opts = [pack_id: Ref.new_pack_id()]

    with {:ok, claims} <- ClaimCheck.check_in(instance, env.tenant_id, [claim_item(env)], opts) do
      {:ok, Map.fetch!(claims, env.id)}
    end
  end

  @doc "The claim-check item for an envelope's body."
  @spec claim_item(Envelope.t()) :: ClaimCheck.item()
  def claim_item(%Envelope{} = env) do
    %{
      id: env.id,
      tenant_id: env.tenant_id,
      body: env.body,
      content_type: env.content_type,
      received_at: env.received_at
    }
  end

  defp claim(%{claim: %{ref: %Ref{}} = claim}, _env), do: {:ok, claim}
  defp claim(ctx, env), do: check_in(ctx.instance, env)

  @doc """
  How the hook was verified, as every sink carries it: `status` is `:ok`,
  `:failed` or `:skipped`, and `flagged` is `true` when a failed check was let
  through by the source's `on_verify_failure: :accept_flag`.

  `:skipped` is a hook nothing checked: a source whose verifier is
  `Ankusa.Verifier.None` (which accepts everything, so its `:ok` says nothing
  about authenticity), or an envelope that never went through ingest (a
  lifecycle event, `verification: nil`). The reason and scheme stay off the
  wire: a failed-but-accepted hook is the consumer's to distrust, not to
  diagnose.
  """
  @spec verification(Ankusa.Verification.t() | nil) :: %{
          status: Ankusa.Verification.status(),
          flagged: boolean()
        }
  def verification(%Ankusa.Verification{provider: Ankusa.Verifier.None}),
    do: %{status: :skipped, flagged: false}

  def verification(%Ankusa.Verification{status: status, flagged: flagged}),
    do: %{status: status, flagged: flagged}

  def verification(nil), do: %{status: :skipped, flagged: false}
end
