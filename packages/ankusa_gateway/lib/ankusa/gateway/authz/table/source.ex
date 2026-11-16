defmodule Ankusa.Gateway.Authz.Table.Source do
  @moduledoc """
  Where a `table` authorizer's rows come from (`gateway.authz.authorizers.<name>.source`):
  a customer's own tables, a permissions file in an object store, or an HTTP
  endpoint.

  A row is a map with at least the five fields `Ankusa.Gateway.Authz.Table`
  names in its `columns` option (`principal`, `action`, `space`, `prefix`,
  `effect` by default), keyed by those column names. The source returns rows
  as it finds them; the authorizer validates every one and counts the bad ones.

  `version/2` (optional) makes a snapshot refresh cheap: equal to the last
  loaded version and nothing is read. `load/4` streams every row through the
  caller's reducer, which returns `{:halt, acc}` to stop early (the snapshot
  stops at its size caps). `lookup/6` (optional) answers one query for one
  request in `mode: :lookup`; request data reaches it only as bound parameters,
  never as SQL text built by the gateway.

  `init!/2` validates the source's options and raises `ArgumentError` naming
  `key`; it runs at configuration time, before any process starts. A source
  that needs a child process (a connection pool) returns it from
  `child_specs/3`.
  """

  @type row :: %{optional(String.t()) => term()}
  @type ctx :: %{instance: atom(), name: String.t(), deadline: integer()}

  @callback init!(opts :: map(), key :: String.t()) :: state :: map()
  @callback child_specs(instance :: atom(), name :: String.t(), state :: map()) :: [
              Supervisor.child_spec()
            ]
  @callback version(state :: map(), ctx()) :: {:ok, term()} | :unknown | {:error, term()}
  @callback load(
              state :: map(),
              ctx(),
              acc,
              (row(), acc -> {:cont, acc} | {:halt, acc})
            ) :: {:ok, acc} | {:error, term()}
            when acc: term()
  @callback lookup(
              state :: map(),
              ctx(),
              identities :: [String.t()],
              action :: String.t(),
              space :: String.t(),
              key :: String.t()
            ) :: {:ok, [row()]} | {:error, term()}

  @optional_callbacks child_specs: 3, version: 2, lookup: 6
end
