defmodule Ankusa.Queue.Reclaim do
  @moduledoc false

  # A hook is deleted once its last obligation clears: every `deliveries` row
  # (pending or dead) and, while the `:storage` role archives, its `?a` key.
  # Whoever clears an obligation writes a `?c` marker in the same batch, then
  # calls `run/2`. If the hook has no obligation left, one batch deletes the
  # hook, its claim-check ref and every marker for it; otherwise only the
  # caller's own marker goes.
  #
  # Two clearers may run at once: each writes its clear *before* it looks, so
  # whichever clear lands last observes both, and every delete is idempotent.
  # A crash between the two batches leaves a marker, which `sweep/1` finds.
  #
  # The probe reads through `Ankusa.Store.fold/6`, which refuses a scan it
  # cannot prove complete. A failed probe therefore deletes nothing: a corrupt
  # or truncated read must never look like "no obligations left".

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @chunk 512

  @doc "Reclaim the hooks behind these `{seq, cleared_key}` markers."
  @spec run(atom(), [{pos_integer(), binary()}]) :: :ok | {:error, term()}
  def run(_instance, []), do: :ok

  def run(instance, pairs) do
    grouped = Enum.group_by(pairs, &elem(&1, 0), &elem(&1, 1))
    seqs = Map.keys(grouped)

    with {:ok, rows?} <-
           Store.prefixes_present(instance, :deliveries, Enum.map(seqs, &<<&1::64>>)),
         {:ok, archive} <-
           Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)) do
      ops =
        [seqs, rows?, archive]
        |> Enum.zip()
        |> Enum.flat_map(fn {seq, rows?, archive} ->
          if rows? or match?({:ok, _}, archive) do
            # Still owed something: only this clearer's own markers go.
            Enum.map(Map.fetch!(grouped, seq), &{:delete, :index, &1})
          else
            reclaim_ops(seq)
          end
        end)

      Store.write(instance, ops, sync: false)
    end
  end

  @doc "Run `run/2` for every marker left in the store (after a crash, or on a timer)."
  @spec sweep(atom()) :: :ok | {:error, term()}
  def sweep(instance) do
    %{lo: lo, hi: hi} = Keys.family(:cleared)

    scan =
      Store.fold(instance, :cleared, {lo, hi}, [], fn key, _value, acc ->
        {seq, _kind, _sink} = Keys.decode_cleared(key)
        {:cont, [{seq, key} | acc]}
      end)

    with {:ok, pairs} <- scan do
      pairs
      |> Enum.chunk_every(@chunk)
      |> Enum.reduce_while(:ok, fn chunk, :ok ->
        case run(instance, chunk) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp reclaim_ops(seq) do
    [
      {:delete, :hooks, Keys.hook(seq)},
      {:delete, :index, Keys.claim(seq)},
      {:delete_range, :index, Keys.cleared(seq, 0, 0), Keys.cleared(seq + 1, 0, 0)}
    ]
  end
end
