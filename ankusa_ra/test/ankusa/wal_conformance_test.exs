defmodule Ankusa.WAL.RaConformanceTest do
  @moduledoc """
  The shared `Ankusa.WAL` contract, run against `Ankusa.WAL.Ra` — the
  single-node shape (`roles: [:edge, :dispatch, :storage, :wal]`), which is the
  honest laptop equivalent of `Ankusa.WAL.DiskLog`.
  """

  use Ankusa.WAL.ConformanceCase,
    adapter: Ankusa.WAL.Conformance.Ra,
    config: fn instance ->
      [wal: {Ankusa.WAL.Ra, members: [{:"ankusa_wal_#{instance}", node()}]}]
    end
end
