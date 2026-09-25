defmodule Ankusa.Envelope do
  @moduledoc """
  The claim-checked record that flows through every layer.

  The envelope is the *only* thing components share. Compaction, dispatch, and
  the dashboard read committed envelopes from the WAL; they never call the edge.

  `body` is the exact raw bytes as received — signature checks need it verbatim.
  Keep it as a refcounted binary and pass it by reference.
  """

  @enforce_keys [:id, :source_id, :received_at, :method, :path, :headers, :body]
  defstruct [
    :id,
    :source_id,
    :tenant_id,
    :received_at,
    :method,
    :path,
    :headers,
    :content_type,
    :body,
    # assigned by the WAL at commit time; nil until durably stored
    :seq,
    # when the WAL took this record for commit, in ms since the epoch. Dedup
    # expiry is measured between these, never against the wall clock at read.
    :committed_at,
    # %Ankusa.Verification{}
    :verification,
    size: 0
  ]

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
          committed_at: integer() | nil,
          verification: Ankusa.Verification.t() | nil,
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

  @doc """
  Serialize an envelope to a compact binary for the WAL.

  `body` is kept verbatim; the term is `:erlang.term_to_binary/2` with
  `:deterministic` so re-encoding a replayed record is byte-stable.
  """
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{} = env) do
    :erlang.term_to_binary(Map.from_struct(env), [:deterministic])
  end

  @doc "Inverse of `to_binary/1`."
  @spec from_binary(binary()) :: t()
  def from_binary(bin) do
    # Trusted, internally-written data. NOT `:safe`: envelopes legitimately carry
    # module atoms (e.g. `verification.provider`) that a fresh decoding node may
    # not have interned yet — `:safe` would reject them and crash replay/fetch.
    map = :erlang.binary_to_term(bin)
    env = struct(__MODULE__, map)
    # Envelopes written before `committed_at` existed decode with it `nil`;
    # fall back to `received_at` so dedup expiry arithmetic never sees `nil`.
    %{env | committed_at: env.committed_at || env.received_at}
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
