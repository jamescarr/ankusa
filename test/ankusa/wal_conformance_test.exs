defmodule Ankusa.WALConformanceTest do
  @moduledoc """
  The shared `Ankusa.WAL` contract, run against `Ankusa.WAL.DiskLog` — the
  default adapter, and the reference for the shared, multi-node ones.
  """

  use Ankusa.WAL.ConformanceCase, adapter: Ankusa.WAL.Conformance.DiskLog
end
