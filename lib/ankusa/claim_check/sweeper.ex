defmodule Ankusa.ClaimCheck.Sweeper do
  @moduledoc """
  Retention for `LocalFS`-backed claims. Only ever started (in the `:storage`
  role) when `claim_check.retention_days` is set —
  `Ankusa.ClaimCheck.validate_config!/1` already rejects that setting against
  any other claim store at boot, so this process only ever runs against
  `BlobStore.LocalFS`.

  Each tick lists every key under the fixed `claims/` prefix, reads the
  UUIDv7 embedded in each claim's own id (`claims/<tenant>/<id>`) for its
  creation time — no extra metadata or `stat` call needed — and deletes
  claims older than `retention_days`. S3/GCS-backed claim stores are never
  covered here; use a bucket lifecycle rule on the `claims/` prefix instead
  (see `docs/claim-check.md`).
  """

  use GenServer

  alias Ankusa.{BlobStore, Config, Telemetry, UUIDv7}

  @claims_prefix "claims/"

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :claim_check_sweeper))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Run one sweep synchronously; returns `{deleted, scanned}`."
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

  defp run_sweep(%{instance: instance, config: config}) do
    started = System.monotonic_time()
    cutoff_ms = System.system_time(:millisecond) - config.claim_check.retention_days * 86_400_000

    keys = BlobStore.list(instance, @claims_prefix)
    expired = Enum.filter(keys, &expired?(&1, cutoff_ms))

    Enum.each(expired, &BlobStore.delete(instance, &1))

    Telemetry.emit(
      [:claim_check, :sweep],
      %{
        deleted: length(expired),
        scanned: length(keys),
        duration: System.monotonic_time() - started
      },
      %{instance: instance}
    )

    {length(expired), length(keys)}
  end

  # A key that isn't a well-formed `claims/<tenant>/<uuidv7>` (or whose id
  # doesn't parse as UUIDv7) is left alone rather than guessed at — retention
  # only ever removes what it can positively date.
  defp expired?(key, cutoff_ms) do
    with [id] <- key |> String.trim_leading(@claims_prefix) |> String.split("/") |> Enum.take(-1),
         {:ok, ms} <- UUIDv7.timestamp_ms(id) do
      ms < cutoff_ms
    else
      _ -> false
    end
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok
end
