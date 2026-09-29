defmodule Ankusa.TestHelpers do
  @moduledoc "Shared helpers for framework tests."

  import ExUnit.Callbacks

  alias Ankusa.Config

  @doc "A unique instance atom for isolation."
  def unique_instance, do: :"t#{System.unique_integer([:positive])}"

  @doc """
  Build an isolated `%Ankusa.Config{}` with a fresh temp `data_dir` (auto-cleaned)
  and an ephemeral HTTP port. Pass `overrides` as keyword to `Ankusa.Config.new/1`.
  """
  def test_config(overrides \\ []) do
    inst = Keyword.get(overrides, :instance, unique_instance())
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}_#{System.unique_integer([:positive])}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(dir) end)

    overrides
    |> Keyword.merge(instance: inst, data_dir: dir)
    |> Keyword.put_new(:port, 0)
    |> Config.new()
  end

  @doc "Put config into persistent_term (needed before starting WAL-facade callers)."
  def put_config(config), do: Ankusa.put_config(config)

  @doc "A free TCP port from a closed listener, for a test that needs one before Bandit binds."
  def free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  @doc """
  Boot a routes-enabled `:edge` instance and return its config.

  `routes_opts` is the keyword under `config.routes` (minus a nested `:admin`
  list, which is merged over `port: 0`). `extra_opts` is merged into the
  top-level config, so a caller can pass `source_store:` or the like.
  """
  def start_routes(routes_opts \\ [], extra_opts \\ []) do
    {admin, routes_opts} = Keyword.pop(routes_opts, :admin, [])

    config =
      test_config(
        Keyword.merge(extra_opts,
          roles: [:edge],
          routes:
            routes_opts
            |> Keyword.put_new(:enabled, true)
            |> Keyword.put(:admin, Keyword.merge([port: 0], admin))
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  @doc "Build a raw ingest request map for `Ankusa.Edge.Ingest`/router."
  def request(source_id, body, headers \\ []) do
    %{
      source_id: source_id,
      method: "POST",
      path: "/webhooks/#{source_id}",
      headers: headers,
      body: body
    }
  end

  @doc "Build valid Standard Webhooks signature headers for `body` and `secret`."
  def standard_webhooks_headers(id, body, secret, ts \\ nil) do
    ts = ts || System.system_time(:second)
    key = secret |> String.replace_prefix("whsec_", "") |> Base.decode64!()
    signed = "#{id}.#{ts}.#{body}"
    sig = Base.encode64(:crypto.mac(:hmac, :sha256, key, signed))

    [
      {"webhook-id", id},
      {"webhook-timestamp", to_string(ts)},
      {"webhook-signature", "v1,#{sig}"}
    ]
  end
end
