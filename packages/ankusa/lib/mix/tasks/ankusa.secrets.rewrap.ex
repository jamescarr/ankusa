defmodule Mix.Tasks.Ankusa.Secrets.Rewrap do
  @shortdoc "Re-encrypts the stored source secrets of a data directory under the current KEK"

  @moduledoc """
  Moves every API-managed source a node stored in its own `Ankusa.Store`
  (`Ankusa.SourceStore.Persistent`) to the current key-encryption key: rows
  written before a KEK existed are encrypted, and rows encrypted under a
  `kek_previous` key are rewritten under the current one. Afterwards the old key
  can be dropped from `secrets.kek_previous`.

      mix ankusa.secrets.rewrap --dry-run
      mix ankusa.secrets.rewrap --data-dir /var/lib/ankusa --instance default

  The store is `<data-dir>/<instance>/store`. Without `--dry-run` the task opens
  the store, so stop the node that owns it first (a running node rewraps with
  `Ankusa.SourceStore.Persistent.rewrap/1`; sources in Redis with
  `Ankusa.SourceStore.Redis.rewrap/1`).

  The keys come from the environment, never the command line:

    * `ANKUSA_KEK` — the current key (`"<base64>"` or `"<id>:<base64>"`)
    * `ANKUSA_KEK_PREVIOUS` — comma-separated keys that may still decrypt

  or, when `ANKUSA_KEK` is unset, from `config :ankusa, :secrets` (the same map
  as `Ankusa.Config`'s `secrets`). A row no key opens stops the task and is
  reported; nothing is written for the rows after it.

  Options:

    * `--dry-run` — count what would be rewritten, change nothing
    * `--data-dir` — default `config :ankusa, :data_dir` (`./data`)
    * `--instance` — default `default`
  """

  use Mix.Task

  alias Ankusa.Crypto.Envelope
  alias Ankusa.SourceStore.Persistent

  @impl Mix.Task
  def run(args) do
    {opts, _argv, invalid} =
      OptionParser.parse(args,
        strict: [dry_run: :boolean, data_dir: :string, instance: :string]
      )

    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")

    Mix.Task.run("app.config")

    data_dir = opts[:data_dir] || Application.get_env(:ankusa, :data_dir, "./data")
    instance = opts[:instance] || "default"

    keyring = keyring!()
    Mix.shell().info("store: #{Path.join([data_dir, instance, "store"])}")
    rewrap(data_dir, instance, keyring, dry_run: opts[:dry_run] == true)
  end

  defp keyring! do
    secrets = secrets()

    config =
      try do
        Ankusa.Config.new(secrets: secrets)
      rescue
        error in ArgumentError -> Mix.raise(error.message)
      end

    case Envelope.keyring(config.secrets) do
      {:ok, %{current: nil}} -> Mix.raise("no KEK: set ANKUSA_KEK (or config :ankusa, :secrets)")
      {:ok, keyring} -> keyring
      {:error, reason} -> Mix.raise("could not read the keys: #{inspect(reason)}")
    end
  end

  defp secrets do
    case System.get_env("ANKUSA_KEK") do
      kek when kek not in [nil, ""] ->
        [kek: kek, kek_previous: split(System.get_env("ANKUSA_KEK_PREVIOUS"))]

      _unset ->
        Application.get_env(:ankusa, :secrets, [])
    end
  end

  defp split(nil), do: []

  defp split(keys) do
    keys |> String.split(",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))
  end

  # The instance name is an operator's command-line argument, never request input.
  # sobelow_skip ["DOS.StringToAtom"]
  defp rewrap(data_dir, instance, keyring, run_opts) do
    # The default instance must not boot (and bind a port) because this task
    # started the application.
    Application.put_env(:ankusa, :autostart, false)
    {:ok, _} = Application.ensure_all_started(:ankusa)

    name = String.to_atom(instance)

    Ankusa.put_config(
      Ankusa.Config.new(instance: name, data_dir: data_dir, roles: [:edge], port: 0)
    )

    # A refused open stops the store with its reason; being linked, this task
    # must trap that exit to report it instead of dying with it.
    Process.flag(:trap_exit, true)

    case Ankusa.Store.start_link(instance: name) do
      {:ok, pid} ->
        result = Persistent.rewrap_rows(name, keyring, run_opts)
        GenServer.stop(pid)
        report(result, keyring, run_opts)

      {:error, reason} ->
        Mix.raise("could not open the store: #{inspect(reason)}")
    end
  end

  defp report({:ok, %{rewrapped: rewrapped, current: current}}, keyring, run_opts) do
    verb = if run_opts[:dry_run], do: "would rewrap", else: "rewrapped"

    Mix.shell().info(
      "#{verb}=#{rewrapped} current=#{current} key=#{Envelope.current_key_id(keyring)}"
    )
  end

  defp report({:error, {:secrets, source_id, reason}}, _keyring, _run_opts),
    do: Mix.raise("source #{source_id} cannot be read with the given keys: #{inspect(reason)}")

  defp report({:error, reason}, _keyring, _run_opts),
    do: Mix.raise("rewrap failed: #{inspect(reason)}")
end
