defmodule Ankusa.Store.Migrations.ClaimLeases do
  @moduledoc false

  # Store format 5 -> 6 (DUR-2.c): the claim leases (`Keys.claim_lease/3`, family
  # `?l`) and their per-tenant `claim_leases` counter (field 9 of the `counters`
  # family). The retention sweeper (`Ankusa.ClaimCheck.Sweeper`) never deletes a
  # pack a lease names.
  #
  # Every `?k` row of a format 5 store is a live hook's claim: the hook is still
  # in the store, or `Ankusa.Queue.Reclaim` would have deleted the row with it.
  # So each one gets a held lease (`<<0::64>>`), as `Deliveries.claim_ops/3` writes
  # at runtime, and is counted in its tenant's and the node's counter, the same
  # merge path. One step reads up to `@batch` `?k` keys after its cursor (the
  # last one it handled; `nil` on the first step). `Ankusa.Store.Migrate` commits a
  # step's ops and its cursor in one synced batch, so a step is counted whole or
  # not at all and a resumed migration never counts a lease twice. A claim whose
  # tenant has no ordinal (none can, but a hand-edited store might) is counted in
  # the node aggregate alone; a row that is not a claim is skipped.

  @behaviour Ankusa.Store.Migration

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 500

  @impl true
  def describe, do: "lease the claim packs of live hooks (rows `?l`, counter `claim_leases`)"

  @impl true
  def step(instance, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:claims)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, rows} <- claim_rows(instance, {from, hi}),
         {:ok, ops} <- lease(instance, rows) do
      if length(rows) < @batch,
        do: {:ok, ops, :done},
        else: {:ok, ops, {:more, rows |> List.last() |> elem(0)}}
    end
  end

  # `[{claim_key, claim}]`, oldest first; a row that is not a claim is left out.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp claim_rows(instance, range) do
    result =
      Store.fold(instance, :claims, range, {0, []}, fn key, value, {n, acc} ->
        acc =
          case :erlang.binary_to_term(value) do
            %{ref: %Ref{}} = claim -> [{key, claim} | acc]
            _ -> [{key, nil} | acc]
          end

        if n + 1 >= @batch, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
      end)

    with {:ok, {_n, rows}} <- result, do: {:ok, Enum.reverse(rows)}
  end

  defp lease(instance, rows) do
    claims = for {<<?k, seq::64>>, %{ref: %Ref{}} = claim} <- rows, do: {seq, claim}
    tenants = claims |> Enum.map(fn {_seq, claim} -> claim.ref.tenant_id end) |> Enum.uniq()

    with {:ok, ords} <- Tenants.lookup(instance, tenants) do
      {known, unknown} =
        Enum.split_with(claims, fn {_seq, claim} ->
          is_integer(Map.get(ords, claim.ref.tenant_id))
        end)

      puts =
        for {seq, claim} <- claims,
            do: {:put, :index, Deliveries.claim_lease_key(claim, seq), <<0::64>>}

      deltas =
        for {_seq, claim} <- known, do: {Map.fetch!(ords, claim.ref.tenant_id), :claim_leases, 1}

      {:ok,
       puts ++
         Deliveries.coalesce(
           Counters.ops(deltas) ++ Counters.node_ops(claim_leases: length(unknown))
         )}
    end
  end
end
