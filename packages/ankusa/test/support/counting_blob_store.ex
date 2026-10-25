defmodule Ankusa.Test.CountingBlobStore do
  @moduledoc """
  A `BlobStore` for tests: `BlobStore.LocalFS` underneath, plus a `{:blob_put,
  key}` message to `:pid` on every `put/4`, so a test can count object writes.

  Opts:

    * `:root` — passed to `LocalFS` (default: the instance's `segments` dir)
    * `:pid` — receives `{:blob_put, key}` per put
    * `:fail_tenant` — every put under that tenant's claims partition fails
    * `:failures` — an `Agent` holding how many upcoming puts to fail
    * `:kill_tenant` — a put under that tenant's claims partition kills the
      process that made it (an upload task that dies, past every `rescue`)
    * `:raise_tenant` — a put under that tenant's claims partition raises
    * `:put_delay_ms` — sleep before each put, to hold a pack upload open
    * `:get_error` — every `get/3` answers `{:error, value}`, an unreachable store
    * `:range_error` — every `get_range/5` answers `{:error, value}`
    * `:range_failures` — an `Agent` holding how many upcoming `get_range/5`
      calls answer `{:error, {:status, 503, "SlowDown"}}`, a throttling store
    * `:range_delay_ms` — sleep before each `get_range/5`, to hold a read open
    * `:notify_range` — `{pid, tag}`: `{:blob_range, tag}` to `pid` on every
      `get_range/5`, so a test can tell which store a read went to
    * `:list_error` — every `list/3` answers `{:error, value}`
    * `:lost_acks` — an `Agent` holding how many upcoming `LATEST` puts land
      but answer `{:error, :timeout}`, as a put whose response was lost
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.BlobStore.LocalFS

  @impl true
  def put(instance, key, data, opts), do: write(instance, key, data, opts, &LocalFS.put/4)

  # An object already under `key` is `{:error, :already_exists}` and stays as it
  # was (unless it holds exactly `data`).
  @impl true
  def put_if_absent(instance, key, data, opts),
    do: write(instance, key, data, opts, &LocalFS.put_if_absent/4)

  defp write(instance, key, data, opts, writer) do
    if pid = Keyword.get(opts, :pid), do: send(pid, {:blob_put, key})
    if delay = Keyword.get(opts, :put_delay_ms), do: Process.sleep(delay)
    if in_tenant?(key, opts[:kill_tenant]), do: Process.exit(self(), :kill)
    if in_tenant?(key, opts[:raise_tenant]), do: raise("injected raise")

    cond do
      fail?(key, opts) ->
        {:error, :injected_failure}

      lost_ack?(key, opts) ->
        with(:ok <- writer.(instance, key, data, local(opts)), do: {:error, :timeout})

      true ->
        writer.(instance, key, data, local(opts))
    end
  end

  defp lost_ack?(key, opts) do
    with true <- String.ends_with?(key, "/LATEST"),
         agent when agent != nil <- Keyword.get(opts, :lost_acks) do
      Agent.get_and_update(agent, fn
        n when n > 0 -> {true, n - 1}
        n -> {false, n}
      end)
    else
      _ -> false
    end
  end

  defp in_tenant?(_key, nil), do: false
  defp in_tenant?(key, tenant), do: String.contains?(key, "tenant=#{tenant}/")

  defp fail?(key, opts) do
    tenant = Keyword.get(opts, :fail_tenant)

    cond do
      tenant && String.contains?(key, "tenant=#{tenant}/") ->
        true

      agent = Keyword.get(opts, :failures) ->
        Agent.get_and_update(agent, fn
          n when n > 0 -> {true, n - 1}
          n -> {false, n}
        end)

      true ->
        false
    end
  end

  @impl true
  def get(instance, key, opts) do
    case Keyword.fetch(opts, :get_error) do
      {:ok, reason} -> {:error, reason}
      :error -> LocalFS.get(instance, key, local(opts))
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    with {pid, tag} <- Keyword.get(opts, :notify_range), do: send(pid, {:blob_range, tag})
    if delay = Keyword.get(opts, :range_delay_ms), do: Process.sleep(delay)

    cond do
      reason = Keyword.get(opts, :range_error) ->
        {:error, reason}

      take(Keyword.get(opts, :range_failures)) ->
        {:error, {:status, 503, "SlowDown"}}

      true ->
        LocalFS.get_range(instance, key, offset, length, local(opts))
    end
  end

  defp take(nil), do: false

  defp take(agent) do
    Agent.get_and_update(agent, fn
      n when n > 0 -> {true, n - 1}
      n -> {false, n}
    end)
  end

  @impl true
  def delete(instance, key, opts), do: LocalFS.delete(instance, key, local(opts))

  @impl true
  def list(instance, prefix, opts) do
    case Keyword.fetch(opts, :list_error) do
      {:ok, reason} -> {:error, reason}
      :error -> LocalFS.list(instance, prefix, local(opts))
    end
  end

  defp local(opts), do: Keyword.take(opts, [:root])
end
