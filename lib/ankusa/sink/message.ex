defmodule Ankusa.Sink.Message do
  @moduledoc """
  The wire format every queue-style sink publishes (`Sink.RabbitMQ`,
  `Sink.Kafka`, `Sink.NATS`), so a consumer parses one format regardless of
  transport.

  A body of at most `inline_max_bytes` (default 64 KiB) rides inline,
  base64-encoded. Anything larger lives in the claim check and the message
  carries its `Ankusa.ClaimCheck.Ref` as one string instead (see
  `docs/claim-check.md`):

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "body_base64": "eyJpZCI6..."}

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 3145728,
       "claim": "urn:ankusa:claim:v1:acme:0199a1c2-...:66:3145728:sha256-9f86d0..."}

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
      case ref(ctx, env) do
        {:ok, ref} -> {:ok, JSON.encode!(Map.put(base, :claim, Ref.to_string(ref)))}
        {:error, reason} -> {:error, {:claim_check, reason}}
      end
    end
  end

  @doc """
  Check `env`'s body in on its own, as a one-claim pack. The pack reuses the
  envelope id (a UUIDv7 from the edge), so a retry rewrites the same object
  instead of orphaning one; an envelope built elsewhere gets a fresh id.
  """
  @spec check_in(atom(), Envelope.t()) :: {:ok, Ref.t()} | {:error, ClaimCheck.reason()}
  def check_in(instance, %Envelope{} = env) do
    opts = if Ref.validate_object_id(env.id) == :ok, do: [object_id: env.id], else: []

    with {:ok, refs} <- ClaimCheck.check_in(instance, env.tenant_id, [claim_item(env)], opts) do
      {:ok, Map.fetch!(refs, env.id)}
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

  defp ref(%{claim: %Ref{} = ref}, _env), do: {:ok, ref}
  defp ref(ctx, env), do: check_in(ctx.instance, env)
end
