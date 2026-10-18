defmodule Ankusa.SecretsRewrapTaskTest do
  @moduledoc """
  `mix ankusa.secrets.rewrap` against the data directory of a stopped node:
  the rotation runbook (new key current, old key previous, rewrap, drop the old
  key) end to end. Not async: the task reads its keys from the environment.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{SourceStore, Store}
  alias Ankusa.SourceStore.Persistent
  alias Mix.Tasks.Ankusa.Secrets.Rewrap, as: RewrapTask

  @kek_a Base.encode64(:binary.copy(<<1>>, 32))
  @kek_b Base.encode64(:binary.copy(<<2>>, 32))

  @spec_map %{
    "verify" => %{"type" => "hmac", "secret" => "whsec-canary", "signature_header" => "X-Sig"},
    "sinks" => [%{"type" => "log"}]
  }

  setup do
    Mix.shell(Mix.Shell.Process)
    previous_env = Application.get_env(:ankusa, :secrets)

    on_exit(fn ->
      Mix.shell(Mix.Shell.IO)
      System.delete_env("ANKUSA_KEK")
      System.delete_env("ANKUSA_KEK_PREVIOUS")

      if previous_env,
        do: Application.put_env(:ankusa, :secrets, previous_env),
        else: Application.delete_env(:ankusa, :secrets)
    end)

    Application.delete_env(:ankusa, :secrets)
    System.delete_env("ANKUSA_KEK")
    System.delete_env("ANKUSA_KEK_PREVIOUS")

    # A node that stored one source under k1, then stopped.
    base = test_config()
    config = node_config(base, kek: "k1:" <> @kek_a)
    put_config(config)
    start_supervised!({Store, instance: config.instance}, id: :first_boot)
    start_supervised!({Ankusa.Audit, instance: config.instance}, id: :first_boot_audit)
    {:ok, pid} = Persistent.start_link(instance: config.instance)
    assert {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
    GenServer.stop(pid)
    stop_supervised!(:first_boot_audit)
    stop_supervised!(:first_boot)

    %{
      base: base,
      args: ["--data-dir", config.data_dir, "--instance", Atom.to_string(config.instance)]
    }
  end

  test "moves a stopped node's rows from the previous key to the current one", ctx do
    System.put_env("ANKUSA_KEK", "k2:" <> @kek_b)
    System.put_env("ANKUSA_KEK_PREVIOUS", "k1:" <> @kek_a)

    RewrapTask.run(["--dry-run" | ctx.args])
    assert_received {:mix_shell, :info, ["store: " <> _]}
    assert_received {:mix_shell, :info, ["would rewrap=1 current=0 key=k2"]}

    RewrapTask.run(ctx.args)
    assert_received {:mix_shell, :info, ["rewrapped=1 current=0 key=k2"]}

    RewrapTask.run(ctx.args)
    assert_received {:mix_shell, :info, ["rewrapped=0 current=1 key=k2"]}

    # The old key is no longer needed: the node boots on k2 alone.
    assert {:ok, %{spec: @spec_map}} = boot_and_get(node_config(ctx.base, kek: "k2:" <> @kek_b))
  end

  test "the dry run changed nothing: the node still needs k1", ctx do
    System.put_env("ANKUSA_KEK", "k2:" <> @kek_b)
    System.put_env("ANKUSA_KEK_PREVIOUS", "k1:" <> @kek_a)
    RewrapTask.run(["--dry-run" | ctx.args])

    assert {:error, {:source_store_load_failed, {:secrets, "acme.billing", {:unknown_key, "k1"}}}} =
             boot_refused(node_config(ctx.base, kek: "k2:" <> @kek_b))
  end

  test "reads the keys from config :ankusa, :secrets when the environment has none", ctx do
    Application.put_env(:ankusa, :secrets, kek: "k2:" <> @kek_b, kek_previous: ["k1:" <> @kek_a])

    RewrapTask.run(ctx.args)
    assert_received {:mix_shell, :info, ["rewrapped=1 current=0 key=k2"]}
  end

  test "without a KEK it stops before opening the store", ctx do
    assert_raise Mix.Error, ~r/no KEK/, fn -> RewrapTask.run(ctx.args) end
  end

  test "a row no given key opens stops the task and names the source", ctx do
    System.put_env("ANKUSA_KEK", "k2:" <> @kek_b)

    error = assert_raise Mix.Error, fn -> RewrapTask.run(ctx.args) end
    assert error.message =~ "source acme.billing cannot be read"
    assert error.message =~ "unknown_key"
    refute error.message =~ @kek_a
  end

  test "a malformed key is refused without printing it", ctx do
    System.put_env("ANKUSA_KEK", "definitely not base64!")

    error = assert_raise Mix.Error, fn -> RewrapTask.run(ctx.args) end
    refute error.message =~ "definitely not base64!"
    assert error.message =~ "secrets.kek must be a base64-encoded 32-byte key"
  end

  test "invalid options are refused", ctx do
    assert_raise Mix.Error, ~r/invalid options/, fn -> RewrapTask.run(["--nope" | ctx.args]) end
  end

  # The node, as a restart under other `secrets` would see it.
  defp node_config(base, secrets) do
    Ankusa.Config.new(
      instance: base.instance,
      data_dir: base.data_dir,
      port: 0,
      source_store: {Persistent, [decoder: &decoder/2]},
      secrets: secrets
    )
  end

  defp boot_and_get(config) do
    put_config(config)
    start_supervised!({Store, instance: config.instance}, id: :boot)
    {:ok, pid} = Persistent.start_link(instance: config.instance)
    result = SourceStore.get(config.instance, "acme", "billing")
    GenServer.stop(pid)
    stop_supervised!(:boot)
    result
  end

  defp boot_refused(config) do
    put_config(config)
    start_supervised!({Store, instance: config.instance}, id: :refused)
    Process.flag(:trap_exit, true)
    result = Persistent.start_link(instance: config.instance)
    stop_supervised!(:refused)
    result
  end

  defp decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]
end
