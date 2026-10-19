defmodule Ankusa.Health do
  @moduledoc """
  Readiness, as `GET /ready` on the ingress and admin listeners answers it.

  `GET /health` is liveness: the listener answers, nothing more. `/ready` asks
  whether this node can ack a hook right now: on a node that runs the local
  store (`Ankusa.Instance.store?/1`), whether the store takes a synced write
  and no write has failed in the last 5 s (`Ankusa.Store.ready/1`; an ingest
  batch refused on a full disk while the probe's few bytes still fit). A full
  disk, a store that is closed or reopening, or a store process that does not
  answer turns the node unready; it turns ready again on its own once writes
  succeed. A node without a store has
  nothing to check and is ready while it runs.

  With `backup.enabled` and `backup.max_age_ms`, the body also says whether the
  last successful backup is recent enough: `backup` is `"fresh"` or `"stale"`
  (older than `max_age_ms`, or the uploader is not running). A stale backup
  leaves the node ready unless `backup.require_fresh` is `true`, in which case
  it answers `503`. Without `max_age_ms` the body has no `backup` key.

  The body never carries a raw error term: `store` is `"ok"`, `"none"`,
  `"store_unavailable"` or `"write_failed"`.
  """

  alias Ankusa.Instance
  alias Ankusa.Store
  alias Ankusa.Store.Backup

  @type body :: %{
          required(:status) => String.t(),
          required(:instance) => String.t(),
          required(:store) => String.t(),
          optional(:backup) => String.t()
        }

  @spec ready(atom()) :: {:ok, body()} | {:error, body()}
  def ready(instance) do
    name = to_string(instance)
    config = Ankusa.config(instance)

    if Instance.store?(config) do
      case Store.ready(instance) do
        :ok ->
          backup_state(
            config,
            %{status: "ready", instance: name, store: "ok"}
          )

        {:error, reason} ->
          {:error, %{status: "unavailable", instance: name, store: label(reason)}}
      end
    else
      {:ok, %{status: "ready", instance: name, store: "none"}}
    end
  end

  # The backup label, and the 503 a stale one earns under `require_fresh`.
  defp backup_state(config, body) do
    case config.backup do
      %{enabled: true, max_age_ms: max_age} when is_integer(max_age) ->
        case Backup.age_ms(config.instance) do
          age when is_integer(age) and age <= max_age ->
            {:ok, Map.put(body, :backup, "fresh")}

          _stale_or_unknown ->
            body = Map.put(body, :backup, "stale")

            if config.backup.require_fresh,
              do: {:error, %{body | status: "unavailable"}},
              else: {:ok, body}
        end

      _no_threshold ->
        {:ok, body}
    end
  end

  defp label(:store_unavailable), do: "store_unavailable"
  defp label(_reason), do: "write_failed"
end
