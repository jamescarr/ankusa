defmodule Ankusa.Queue.Reclaim do
  @moduledoc false

  # A hook is deleted once its last obligation clears: every `deliveries` row
  # (pending or dead) and, while the `:storage` role archives, its `?a` key.
  # Whoever clears an obligation writes a `?c` marker in the same batch, then
  # calls `run/2`. If the hook has no obligation left, one batch deletes the
  # hook, its claim-check ref and every marker for it, and turns the claim
  # pack's lease from held into a grace lease that runs `claim_check.redeem_grace_ms`
  # past now (`Keys.claim_lease/3`): "delivered" to a queue sink only means the
  # broker took a message that carries the ref, so the pack must outlive the
  # consumer that redeems it. Otherwise only the caller's own marker goes.
  #
  # Two clearers may run at once: each writes its clear *before* it looks, so
  # whichever clear lands last observes both, and every delete is idempotent.
  # A crash between the two batches leaves a marker, which `sweep/1` finds.
  #
  # The probe reads through `Ankusa.Store.fold/6`, which refuses a scan it
  # cannot prove complete. A failed probe therefore deletes nothing: a corrupt
  # or truncated read must never look like "no obligations left".

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Queue.Deliveries
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
           Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)),
         owed = owed_seqs(seqs, rows?, archive),
         {:ok, leases} <- grace_leases(instance, Enum.reject(seqs, &MapSet.member?(owed, &1))) do
      ops =
        Enum.flat_map(seqs, fn seq ->
          if MapSet.member?(owed, seq) do
            # Still owed something: only this clearer's own markers go.
            Enum.map(Map.fetch!(grouped, seq), &{:delete, :index, &1})
          else
            reclaim_ops(seq) ++ Map.get(leases, seq, [])
          end
        end)

      Store.write(instance, ops, sync: false)
    end
  end

  defp owed_seqs(seqs, rows?, archive) do
    [seqs, rows?, archive]
    |> Enum.zip()
    |> Enum.filter(fn {_seq, rows?, archive} -> rows? or match?({:ok, _}, archive) end)
    |> MapSet.new(&elem(&1, 0))
  end

  # `%{seq => [lease op]}` for the hooks about to be deleted that hold a claim:
  # the lease their check-in wrote becomes a grace lease. A failed read deletes
  # nothing, like a failed probe.
  defp grace_leases(_instance, []), do: {:ok, %{}}

  defp grace_leases(instance, seqs) do
    with {:ok, claims} <- Store.multi_get(instance, :index, Enum.map(seqs, &Keys.claim/1)) do
      expires = System.system_time(:millisecond) + redeem_grace_ms(instance)

      leases =
        for {seq, {:ok, bin}} <- Enum.zip(seqs, claims),
            %{ref: %Ref{}} = claim <- [decode_claim(bin)],
            into: %{} do
          {seq, [{:put, :index, Deliveries.claim_lease_key(claim, seq), <<expires::64>>}]}
        end

      {:ok, leases}
    end
  end

  defp redeem_grace_ms(instance),
    do: Ankusa.config(instance).claim_check.redeem_grace_ms

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_claim(bin), do: :erlang.binary_to_term(bin)

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
