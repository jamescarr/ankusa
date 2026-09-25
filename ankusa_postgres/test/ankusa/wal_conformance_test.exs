defmodule Ankusa.WAL.PostgresConformanceTest do
  @moduledoc """
  The shared `Ankusa.WAL` contract, run against `Ankusa.WAL.Postgres`.

  Requires a live Postgres: `docker compose up -d --wait` in this directory.
  """

  use Ankusa.WAL.ConformanceCase, adapter: Ankusa.WAL.Conformance.Postgres
end
