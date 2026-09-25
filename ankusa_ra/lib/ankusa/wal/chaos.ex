defmodule Ankusa.WAL.Chaos do
  @moduledoc """
  Tooling for the chaos harness (`ankusa_ra/chaos`), shipped with the package so
  the harness can use the same code as the test suites rather than a copy.

  Operators can use it too: `scan/1` is "everything the log will still let me
  read", which is what you want after an incident.

  Nothing here is on a hot path, and nothing here is part of the `Ankusa.WAL`
  contract.
  """

  alias Ankusa.Envelope

  @page 500

  @doc """
  Every live record as a list of `%{"id" =>, "seq" =>, "sha256" =>}` maps.

  Reads through `Ankusa.WAL`, so it sees exactly what a reader sees — including
  that a truncated record is gone, which is the point.
  """
  @spec scan(atom()) :: [map()]
  def scan(instance \\ :default) do
    before = instance |> Ankusa.WAL.stats() |> Map.get(:records, 0)
    rows = scan(instance, 0, [])
    after_live = instance |> Ankusa.WAL.stats() |> Map.get(:records, 0)

    # A read that cannot be answered comes back `[]` — right for the dispatch
    # pipeline, which retries on its next tick, wrong for a scan, which would
    # report an empty (or short) log because of one transient failure. So compare
    # against what the replicated state says is live: an unmatched count on a log
    # that did not change underneath the scan means reads were lost, and saying
    # so beats quietly under-reporting during an incident.
    #
    # If the log *did* move — a compactor truncating, clients appending — no
    # completeness claim is available either way, and the scan stays quiet rather
    # than crying wolf.
    if before == after_live and length(rows) != after_live do
      raise "the WAL holds #{after_live} live records but the scan read #{length(rows)}: " <>
              "a read failed"
    end

    rows
  end

  defp scan(instance, cursor, acc) do
    case Ankusa.WAL.read(instance, cursor, @page) do
      [] ->
        Enum.reverse(acc)

      records ->
        acc = Enum.reduce(records, acc, fn env, acc -> [row(env) | acc] end)
        scan(instance, List.last(records).seq, acc)
    end
  end

  @doc "`scan/1` as JSON, for a script that captures it to a file."
  @spec dump(atom()) :: String.t()
  def dump(instance \\ :default), do: JSON.encode!(scan(instance))

  defp row(%Envelope{} = env) do
    %{"id" => env.id, "seq" => env.seq, "sha256" => sha256(env.body)}
  end

  @doc "Lowercase hex SHA-256 of a body, the same digest the load generator records."
  @spec sha256(binary()) :: String.t()
  def sha256(body), do: Base.encode16(:crypto.hash(:sha256, body), case: :lower)
end
