defmodule Ankusa.Envelope do
  @moduledoc """
  The claim-checked record that flows through every layer.

  The envelope is the *only* thing components share. Compaction, dispatch, and
  the dashboard read committed envelopes from the store; they never call the edge.

  `body` is the exact raw bytes as received — signature checks need it verbatim.
  Keep it as a refcounted binary and pass it by reference.
  """

  # The single list of fields: `defstruct` and the decoder's allow-list.
  @fields [
    :id,
    :source_id,
    :tenant_id,
    :received_at,
    :method,
    :path,
    :headers,
    :content_type,
    :body,
    # assigned by `Ankusa.Queue.Writer` at commit time; nil until durably stored
    :seq,
    # %Ankusa.Verification{}
    :verification,
    # provider event key extracted at ingest (Ankusa.Dedupe.key/2); nil when
    # the source has no dedupe or the stamp is missing
    :dedupe_key,
    # The W3C trace context the hook travels under (`Ankusa.Trace`): the
    # request's `traceparent`/`tracestate`, or the ingest span's when an SDK
    # runs; nil without either. Additive (F7): a node that predates it ignores it.
    :trace,
    size: 0
  ]

  @known_keys Enum.map(@fields, fn
                {key, _default} -> key
                key -> key
              end)

  # The encoding `to_binary/1` writes. 0 = the bare term (readable by 0.6 and
  # earlier); 1 = `"AE", 1, term`. Flips to 1 one minor after readers know v1.
  @write_version 0

  @enforce_keys [:id, :source_id, :received_at, :method, :path, :headers, :body]
  defstruct @fields

  @type t :: %__MODULE__{
          id: String.t(),
          source_id: String.t(),
          tenant_id: String.t() | nil,
          received_at: integer(),
          method: String.t(),
          path: String.t(),
          headers: [{String.t(), String.t()}],
          content_type: String.t() | nil,
          body: binary(),
          seq: non_neg_integer() | nil,
          verification: Ankusa.Verification.t() | nil,
          dedupe_key: String.t() | nil,
          trace: Ankusa.Trace.t() | nil,
          size: non_neg_integer()
        }

  @doc "Fetch the first value of a request header (case-insensitive)."
  @spec header(t(), String.t()) :: String.t() | nil
  def header(%__MODULE__{headers: headers}, name) do
    name = String.downcase(name)

    Enum.find_value(headers, fn {k, v} ->
      if String.downcase(k) == name, do: v
    end)
  end

  @doc "The tenant-scoped key a consumer dedupes this hook on; see docs/delivery.md."
  @spec idempotency_key(t()) :: String.t()
  def idempotency_key(%__MODULE__{dedupe_key: key} = env) when is_binary(key) and key != "",
    do: (env.tenant_id || "default") <> ":" <> env.source_id <> ":" <> key

  def idempotency_key(%__MODULE__{id: id}), do: id

  @doc """
  Serialize an envelope to a compact binary for the store.

  `body` is kept verbatim; the term is `:erlang.term_to_binary/2` with
  `:deterministic` so re-encoding a replayed record is byte-stable.

  ## Versions

  * `0` (default): the bare term, as every release up to 0.6 wrote it.
  * `1` (`version: 1`): `<<"AE", 1, term::binary>>`. Nothing writes it by
    default yet: this release reads both, the next one flips the default, so a
    rollback from this release to the previous one still reads every row it
    wrote (see `docs/deployment.md`, "Rolling back a minor").
  """
  @spec to_binary(t(), keyword()) :: binary()
  def to_binary(%__MODULE__{} = env, opts \\ []) do
    term = :erlang.term_to_binary(Map.from_struct(env), [:deterministic])

    case Keyword.get(opts, :version, @write_version) do
      0 -> term
      1 -> <<"AE", 1, term::binary>>
    end
  end

  @doc """
  Inverse of `to_binary/1,2`: reads the unprefixed legacy form (v0) and v1.

  Keys the struct does not know (a field added by a newer node) are ignored,
  so a row written by N+1 decodes on N. A version this node does not know is
  `{:error, :unsupported_envelope_version}`; bytes that are no envelope at all
  are `{:error, :undecodable}`. Never raises: callers dead-letter or log.
  """
  @spec from_binary(binary()) ::
          {:ok, t()} | {:error, :unsupported_envelope_version | :undecodable}
  def from_binary(<<"AE", 1, term::binary>>), do: decode(term)
  def from_binary(<<"AE", _version, _term::binary>>), do: {:error, :unsupported_envelope_version}
  def from_binary(bin) when is_binary(bin), do: decode(bin)

  # Trusted, internally written data (this node's own store rows); never request input.
  # Not `:safe` on purpose: envelopes carry module atoms (`verification.provider`)
  # and a newer node's added keys, atoms this VM may not have interned yet;
  # `:safe` would reject the whole row instead of ignoring the unknown field.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode(term) do
    case :erlang.binary_to_term(term) do
      %{} = map -> {:ok, struct(__MODULE__, Map.take(map, @known_keys))}
      _ -> {:error, :undecodable}
    end
  rescue
    ArgumentError -> {:error, :undecodable}
  end
end

defmodule Ankusa.Verification do
  @moduledoc "Result of running a `Ankusa.Verifier` against an envelope."

  defstruct status: :skipped, provider: nil, scheme: nil, reason: nil, flagged: false

  @type status :: :ok | :failed | :skipped
  @type t :: %__MODULE__{
          status: status(),
          provider: module() | nil,
          scheme: String.t() | nil,
          reason: term(),
          flagged: boolean()
        }
end
