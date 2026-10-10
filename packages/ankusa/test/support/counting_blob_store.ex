defmodule Ankusa.Test.CountingBlobStore do
  @moduledoc """
  A `BlobStore` for tests: `BlobStore.LocalFS` underneath, plus a `{:blob_put,
  key}` message to `:pid` on every `put/4`, so a test can count object writes.

  Opts:

    * `:root` — passed to `LocalFS` (default: the instance's `segments` dir)
    * `:pid` — receives `{:blob_put, key}` per put
    * `:fail_tenant` — every put under that tenant's claims partition fails
    * `:failures` — an `Agent` holding how many upcoming puts to fail
    * `:put_delay_ms` — sleep before each put, to hold a pack upload open
    * `:get_error` — every `get/3` answers `{:error, value}`, an unreachable store
    * `:list_error` — every `list/3` answers `{:error, value}`
    * `:lost_acks` — an `Agent` holding how many upcoming `LATEST` puts land
      but answer `{:error, :timeout}`, as a put whose response was lost
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.BlobStore.LocalFS

  @impl true
  def put(instance, key, data, opts) do
    if pid = Keyword.get(opts, :pid), do: send(pid, {:blob_put, key})
    if delay = Keyword.get(opts, :put_delay_ms), do: Process.sleep(delay)

    cond do
      fail?(key, opts) ->
        {:error, :injected_failure}

      lost_ack?(key, opts) ->
        with(:ok <- LocalFS.put(instance, key, data, local(opts)), do: {:error, :timeout})

      true ->
        LocalFS.put(instance, key, data, local(opts))
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
  def get_range(instance, key, offset, length, opts),
    do: LocalFS.get_range(instance, key, offset, length, local(opts))

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
