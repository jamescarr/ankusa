defmodule Ankusa.TestHelpers do
  @moduledoc "Shared helpers for framework tests."

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
