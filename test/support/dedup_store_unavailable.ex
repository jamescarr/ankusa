defmodule Ankusa.DedupStore.Unavailable do
  @moduledoc """
  A test store whose ledger can be switched off.

  It is the only way to exercise the third answer `Ankusa.DedupStore.record/6`
  has: a store that cannot decide must not be guessed at. `up:` is anything that
  answers whether the ledger is reachable — a pid to call, an Agent, a fun — so
  a test can take the ledger away mid-run and give it back.

  While it is up it behaves exactly like `Ankusa.DedupStore.ETS`.
  """

  @behaviour Ankusa.DedupStore

  alias Ankusa.DedupStore

  defstruct [:up, :table]

  @type t :: %__MODULE__{up: (-> boolean()), table: :ets.tid()}

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      up: Keyword.fetch!(opts, :up),
      table: :ets.new(:ankusa_dedup_unavailable, [:set, :public])
    }
  end

  @impl DedupStore
  @spec record(t(), DedupStore.scope(), String.t(), pos_integer(), integer(), pos_integer()) ::
          :deliver | :drop | {:error, term()}
  def record(%__MODULE__{} = store, scope, key, seq, committed_at, ttl_ms) do
    if up?(store.up) do
      DedupStore.ETS.record(
        %DedupStore.ETS{table: store.table, sweep_delta: 10_000},
        scope,
        key,
        seq,
        committed_at,
        ttl_ms
      )
    else
      {:error, :unavailable}
    end
  end

  defp up?(pid) when is_pid(pid), do: Agent.get(pid, & &1)
  defp up?(fun) when is_function(fun, 0), do: fun.()
end
