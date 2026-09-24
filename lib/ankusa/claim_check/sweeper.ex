defmodule Ankusa.ClaimCheck.Sweeper do
  @moduledoc """
  Retention for `LocalFS`-backed claims. Only ever started (in the `:storage`
  role) when `claim_check.retention_days` is set —
  `Ankusa.ClaimCheck.validate_config!/1` already rejects that setting against
  any other blob store at boot, so this process only ever runs against
  `BlobStore.LocalFS`.

  Claim objects live in day partitions, `claims/tenant=<t>/dt=<yyyy-mm-dd>/`,
  so each tick removes whole partition directories: a day is deleted once every
  object in it is older than `retention_days`, which means a claim is always
  kept for at least that long. Nothing is listed object by object. A directory
  that isn't a well-formed partition is left alone rather than guessed at.
  S3/GCS-backed claims are never covered here; use a bucket lifecycle rule on
  the `claims/` prefix instead (see `docs/claim-check.md`).
  """

  use GenServer

  alias Ankusa.{Config, Telemetry}
  alias Ankusa.ClaimCheck.Ref

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :claim_check_sweeper))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Run one sweep synchronously; returns `{deleted, scanned}` day partitions."
  @spec sweep(atom()) :: {non_neg_integer(), non_neg_integer()}
  def sweep(instance),
    do: GenServer.call(Ankusa.via(instance, :claim_check_sweeper), :sweep, :infinity)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Keyword.fetch!(opts, :config)
    interval = config.claim_check.sweep_interval_ms
    schedule(interval)
    {:ok, %{instance: instance, config: config, interval: interval}}
  end

  # ── ticks ─────────────────────────────────────────────────────────────────

  @impl true
  def handle_call(:sweep, _from, state) do
    result = run_sweep(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:tick, state) do
    run_sweep(state)
    schedule(state.interval)
    {:noreply, state}
  end

  # ── sweep ─────────────────────────────────────────────────────────────────

  defp run_sweep(%{config: config} = state) do
    started = System.monotonic_time()

    # A day strictly before this date holds only objects past retention.
    cutoff =
      (System.system_time(:millisecond) - config.claim_check.retention_days * 86_400_000)
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_date()

    partitions = partitions(config)
    expired = Enum.filter(partitions, fn {_dir, date} -> Date.compare(date, cutoff) == :lt end)

    Enum.each(expired, fn {dir, _date} -> File.rm_rf!(dir) end)

    Telemetry.emit(
      [:claim_check, :sweep],
      %{
        deleted: length(expired),
        scanned: length(partitions),
        duration: System.monotonic_time() - started
      },
      %{instance: state.instance}
    )

    {length(expired), length(partitions)}
  end

  # Every `claims/tenant=*/dt=*` directory under the LocalFS root, with its date.
  defp partitions(config) do
    [Config.path(config, "segments"), Ref.claims_prefix(), "tenant=*", "dt=*"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.filter(&File.dir?/1)
    |> Enum.flat_map(fn dir ->
      with "dt=" <> date <- Path.basename(dir),
           {:ok, date} <- Date.from_iso8601(date) do
        [{dir, date}]
      else
        _ -> []
      end
    end)
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok
end
