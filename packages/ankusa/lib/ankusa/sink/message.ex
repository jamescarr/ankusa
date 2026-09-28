defmodule Ankusa.Sink.Message do
  @moduledoc """
  The wire format every queue-style sink publishes (`Sink.RabbitMQ`,
  `Sink.Kafka`, `Sink.NATS`), so a consumer parses one format regardless of
  transport.

  A body of at most `inline_max_bytes` (default 64 KiB) rides inline,
  base64-encoded. Anything larger lives in the claim check and the message
  carries its `Ankusa.ClaimCheck.Ref` as one string instead, plus the
  lowercase hex sha256 the reader checks the redeemed bytes against (see
  `docs/claim-check.md`):

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "body_base64": "eyJpZCI6..."}

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 3145728,
       "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002",
       "sha256": "3bea8a9a07c1e8dc..."}

  `v` changes only when an existing field changes meaning or disappears.
  Adding a field keeps `v: 1`; consumers must ignore keys they don't know.

  Base64 inflates the inline body by 4/3, so `inline_max_bytes * 4/3` plus
  ~1 KiB of envelope must stay under the smallest message limit on the path
  (Kafka `max.message.bytes`, 1 MiB by default; NATS `max_payload`, 1 MiB;
  SQS, 256 KiB). The 64 KiB default is about 88 KiB encoded, under all of
  them. Nothing here enforces it: an oversized message fails at the broker and
  goes through the source's retry policy like any other sink error.
  """

  alias Ankusa.{ClaimCheck, Envelope}
  alias Ankusa.ClaimCheck.Ref

  @default_inline_max_bytes 65_536

  @doc "The default `inline_max_bytes` for every queue-style sink: 64 KiB."
  @spec default_inline_max_bytes() :: pos_integer()
  def default_inline_max_bytes, do: @default_inline_max_bytes

  @doc "A sink's configured `:inline_max_bytes`, or the default."
  @spec inline_max_bytes(keyword()) :: pos_integer()
  def inline_max_bytes(opts), do: Keyword.get(opts, :inline_max_bytes, @default_inline_max_bytes)

  @doc """
  Encode `env` for a sink whose threshold is `inline_max_bytes`.

  A body over the threshold uses the ref dispatch already checked in
  (`ctx.claim`). A sink called outside dispatch has none, so the body is
  checked in here, as a one-claim pack.
  """
  @spec encode(Envelope.t(), Ankusa.Sink.ctx(), pos_integer()) ::
          {:ok, binary()} | {:error, {:claim_check, ClaimCheck.reason()}}
  def encode(%Envelope{} = env, ctx, inline_max_bytes)
      when is_integer(inline_max_bytes) and inline_max_bytes > 0 do
    base = %{
      v: 1,
      id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      received_at: env.received_at,
      content_type: env.content_type,
      size: env.size
    }

    if env.size <= inline_max_bytes do
      {:ok, JSON.encode!(Map.put(base, :body_base64, Base.encode64(env.body)))}
    else
      case claim(ctx, env) do
        {:ok, %{ref: ref, sha256: sha256}} ->
          {:ok, JSON.encode!(Map.merge(base, %{claim: Ref.to_string(ref), sha256: sha256}))}

        {:error, reason} ->
          {:error, {:claim_check, reason}}
      end
    end
  end

  @doc """
  Check `env`'s body in on its own, as a one-claim pack. The pack id is
  derived from the envelope (its receive time and a hash of its id), so a
  retry rewrites the same object instead of orphaning one.
  """
  @spec check_in(atom(), Envelope.t()) ::
          {:ok, ClaimCheck.claim()} | {:error, ClaimCheck.reason()}
  def check_in(instance, %Envelope{} = env) do
    <<entropy::binary-8, _::binary>> = :crypto.hash(:sha256, env.id)
    opts = [pack_id: Ref.pack_id(env.received_at, entropy)]

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
end
